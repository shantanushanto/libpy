import os
import uuid
import time
import dill
import sys
import random
import itertools
import subprocess
from typing import List, Union
from tqdm import tqdm
from collections import defaultdict
from copy import deepcopy

from libpy import pyutils, commonutils
from libpy.pyutils import ActionRouter


class Slurm:

    user = 'shanto'

    RUNNING = 'RUNNING'
    PENDING = 'PENDING'

    def __init__(self, id, name, status, task: 'Task' = None):
        self.id = id
        self.name = name
        self.status = status

        self._task = task  # task object

    @property
    def task(self):
        return self._task

    @task.setter
    def task(self, t):
        self._task = t

    def has_task(self):
        return True if self.task else False

    @staticmethod
    # sbatch file
    def sbatch(file, prod=False, verbose=False):
        if not prod:
            pyutils.errprint('Testing sbatch..', time_stamp=False)
            return False

        try:
            cmd = f'sbatch {file}'
            command = cmd.split(' ')
            result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
            if verbose and 'error' in result.stderr:
                pyutils.errprint(f'SlurmError: {result.stderr}')
                return False
            time.sleep(0.01)
            return True
        except:
            if verbose:
                pyutils.errprint(f"SlurmError: in sbatch [{cmd}]", time_stamp=False)
            return False

    @staticmethod
    # cancel by status or job_id. job id either in int or string. For multiple job id must be string comma seperated
    def scancel(opt: str, jobids=None, prod=False):
        # opt : pending, all, id, id_pending (cancel only if id in PENDING)
        scancel_pre = f'scancel -u {Slurm.user}'
        if opt == 'all':
            cmd = f'{scancel_pre}'  # scancel -u shanto
        elif opt == 'pending':
            cmd = f'{scancel_pre} --state PENDING'  # scancel -u shanto --state PENDING
        elif opt in ['id', 'id_pending']:
            # cancelling only pending id
            if opt == 'id_pending':
                scancel_pre = f'{scancel_pre} --state PENDING'  # --quiet flag ignore if any prob occurs

            # scancel your_job-id1, your_job-id2, your_jobiid3
            if jobids == None:
                raise ValueError('scancel id is passed as opt but jobids is None')
            elif type(jobids) == int or type(jobids) == str:
                cmd = f'{scancel_pre} {jobids}'
            elif type(jobids) == list:
                if len(jobids) > 0:
                    p = " ".join(str(id) for id in jobids)
                    cmd = f'{scancel_pre} {p}'
                else:
                    pyutils.errprint('No job id to cancel', time_stamp=False)
                    return
            else:
                raise ValueError('Invalid jobids type')
        else:
            raise ValueError('Invalid type')

        if prod:
            try:
                command = cmd.split(' ')
                result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                        universal_newlines=True)
                if 'error' in result.stderr:
                    pyutils.errprint(f'SlurmError: {result.stderr}', time_stamp=False)
                    return False
                time.sleep(0.01)
                return True
            except:
                pyutils.errprint(result.stderr, time_stamp=False)
                pyutils.errprint(f"SlurmError: in scancel [{cmd}]", time_stamp=False)
                return False

    @staticmethod
    # job cancel by string/regular expression name
    def scancel_name(reg_name: str, jobs: List[dict]):
        # reg_name: regular expression string
        # jobs: jobs is a dict list (id, name)
        import re

        reg = re.compile(reg_name)
        # job to be canceled id and name
        ids, names = [], []

        # get job id that match with reg_name
        for job in jobs:
            id = job['id']
            name = job['name']

            if reg.search(name):
                ids.append(id)
                names.append(name)

        # callbacks for ActionRouter
        # Show details of jobs
        def callback_show_details(*args, **kwargs):
            ids = kwargs['ids']
            names = kwargs['names']
            for id, name in zip(ids, names):
                print(id, name)
            # if yes is selected call that action again
            return {"type": "recall"}

        # cancel all jobs
        def callback_cancel(*args, **kwargs):
            Slurm.scancel(opt='id', jobids=kwargs['ids'], prod=True)

        pyutils.ActionRouter(header=f'Total job to be canceled: {len(ids)}') \
            .add('show_details', callback_show_details, ids=ids, names=names) \
            .add('cancel_direct', callback_cancel, ids=ids) \
            .ask()

    # get job that is in the sbatch running or pending
    @staticmethod
    def get_jobs_in_sbatch(by_status=None):
        # by_status: PENDING, RUNNING

        # return job in a list. job is dict of {id, status, name}
        def parse(lines, by_status):
            lines = [line.strip().strip('"') for line in lines.split('\n') if len(line.strip().strip('"')) > 0]

            jobs = []
            for line in lines:
                u = line.split(' ')
                job = Slurm(id=int(u[0]), status=u[1], name=u[2])
                if by_status:
                    if by_status == job.status:
                        jobs.append(job)
                else:
                    jobs.append(job)

            return jobs

        cmd = ['squeue', '-u', f'{Slurm.user}', '--format="%i %T %j"', '--noheader']  # id, status, job_fullname
        try:
            result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
            if 'error' in result.stderr:
                raise ValueError(f'SlurmError: {result.stderr}')
            jobs = parse(lines=result.stdout, by_status=by_status)
            return jobs
        except:
            pyutils.errprint(f"SlurmError: in getting Job id [{' '.join(cmd)}]", time_stamp=False)
            return None

    @staticmethod
    # get available resource
    def get_resource(no_cpu, opt='free'):
        # opt: all, free

        # given no_cpu to run each job it returns partition to submit.
        def free_cpu_block(blocks, no_cpu):
            free_block = []
            for block in blocks:
                cpu_avail = int((block['cpu_tot'] - block['cpu_alloc']) / no_cpu)
                for _ in range(cpu_avail):
                    free_block.append(block['partition'])

            return free_block

        # parse free blocks from bash command
        def parse(lines):
            lines = [line.strip().strip('"') for line in lines.split('\n') if len(line.strip().strip('"')) > 0]
            blocks = []
            block = {}

            for no_line, line in enumerate(lines):
                line = line.lstrip()
                # add new block
                if line.startswith("NodeName=") and len(block) > 0:
                    blocks.append(block)
                    block = {}

                # find total and allocated cpu
                if line.startswith("CPUAlloc="):
                    words = line.split(' ')
                    block['cpu_alloc'] = int(words[0].split('=')[1])
                    block['cpu_tot'] = int(words[1].split('=')[1])

                # find partition name
                if line.startswith("Partitions="):
                    words = line.split('=')
                    block['partition'] = words[1].strip()

            return blocks

        cmd = ["scontrol", 'show', 'node']
        try:
            # get free partition name counted by no_cpu needed
            result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
            if 'error' in result.stderr:
                raise ValueError(f'SlurmError: {result.stderr}')
            blocks = parse(lines=result.stdout)
            if opt == 'all':
                return blocks
            elif opt == 'free':
                free_blocks = free_cpu_block(blocks=blocks, no_cpu=no_cpu)
                return free_blocks
            else:
                raise ValueError('Invalid option')
        except:
            pyutils.errprint(f"SlurmError: in getting cluster available resourcess [{' '.join(cmd)}]", time_stamp=False)
            return []

    @staticmethod
    # get pending/running Slurm object with task inside. intersection of task and jobs
    def get_slurm_task(tasks: List['Task'], ret_by=None, by_status=None):
        # tasks: list of Task object
        # ret_by: TaskId (task and id in separate list). by_status must be given
        # by_status: if given return list
        # return: list of SlurmTask object in dictionary by status.
        #         Also has a 'NOT_IN_SLURM' category that is (tasks - not_slurm_job)
        NOT_IN_SLURM = 'NOT_IN_SLURM'
        slurm_tasks = defaultdict(list)  # {[Slurm]}, category by job_status. Each list contain dict{task, id}

        jobs_in_cluster = Slurm.get_jobs_in_sbatch(by_status=by_status)

        # sorting by their name
        jobs_in_cluster = sorted(jobs_in_cluster, key=lambda x: x.name)
        tasks = sorted(tasks, key=lambda x: x.file_name())

        # with two pointer search in sorted jobs and tasks name to achieve order O(n)
        jidx, tidx = 0, 0
        while jidx < len(jobs_in_cluster) and tidx < len(tasks):
            job = jobs_in_cluster[jidx]
            task = tasks[tidx]
            '''
            Case 1: [e g x] [a d e f g h z] -> [e, g]
            Case 2: [z] [a b] -> []
            Case 3: [b c m n] [a e f g m] -> [m]
            '''
            if job.name == task.file_name():
                slurmtask = SlurmTask(task=task, slurm=job)
                slurm_tasks[job.status].append(slurmtask)
                jidx += 1
                tidx += 1
            elif job.name > task.file_name():
                slurm_tasks[NOT_IN_SLURM].append(task)
                tidx += 1
            elif job.name < task.file_name():
                jidx += 1

        # extra tasks that is not in job
        for idx in range(tidx, len(tasks)):
            slurm_tasks[NOT_IN_SLURM].append(tasks[idx])

        # if TaskId: return task and slurm id in seperate list
        if ret_by == 'TaskId':
            if by_status is None:
                raise ValueError('For TaskId option status need to be specified.')
            else:
                tasks, slurm_ids = [], []
                for slurm_task in slurm_tasks[by_status]:
                    tasks.append(slurm_task.task)
                    slurm_ids.append(slurm_task.id)
                return tasks, slurm_ids

        if by_status:
            return slurm_tasks[by_status]

        return slurm_tasks

    @staticmethod
    # get individual attr val in a list given all Slurm Job.
    def get_attr(jobs, by='name'):
        vals = []
        for job in jobs:
            if by == 'name':
                vals.append(job.name)
            elif by == 'id':
                vals.append(job.id)
            else:
                raise ValueError('Invalid attr value')

        return vals


class Task:
    def __init__(self, cmd, out):
        self.cmd = cmd
        self.out = out  # full path of out file

    def __str__(self):
        return f'{self.cmd} -> {self.out}'

    # get only file name
    def file_name(self):
        return os.path.basename(self.out)

    @staticmethod
    def cache_tasks(tasks, dir):
        # cache list of tasks.
        task_cache_file = os.path.join(dir,
                                       f'.tasks_{time.time()}_n-task-{len(tasks)}_{time.ctime().replace(" ", "-")}.tasks')
        with open(task_cache_file, 'wb') as file:
            dill.dump(tasks, file)

    @staticmethod
    def incomplete_tasks_from_cache(dir, file_name=None, finish_tag=pyutils.tag_job_finished_successfully,
                                    verbose=True):
        # given dir get recent submitted tasks which are not completed
        # dir: to look for all generated data and previously submitted tasks cache
        # file_name: if file_name is None use recent tasks cache file. To use otherwise only file_name (without full path, dir provide path) need to be passed.
        # finish_tag: in err file to check if the task is completed

        def get_tasks():
            # get task from recent or given file
            # get recent one
            if file_name is None:
                files = pyutils.files_with_extension(dir, '.tasks')
                # get recent one
                recent_file = sorted(files)[-1]
            else:
                recent_file = os.path.join(dir, file_name)

            with open(recent_file, 'rb') as file:
                tasks = dill.load(file)

            return tasks

        def get_incomplete_task(submitted_tasks):
            # jobs that are incomplete. Either not file exist for that or success tag doesn't exist
            incomplete_tasks = []
            # check finish tag from each file or existence of file
            for task in submitted_tasks:
                # check if job is finished
                if not pyutils.is_job_finished(file_path=task.out):
                    incomplete_tasks.append(task)

            return incomplete_tasks

        # previously submitted task
        submitted_tasks = get_tasks()
        # find incomplete task
        incomplete = get_incomplete_task(submitted_tasks)
        if verbose: sys.stderr.write(f'Total incomplte task: {len(incomplete)}\n')
        return incomplete

    @staticmethod
    # Given list of tasks and name return if any of the task match the name
    def find(tasks: List['Task'], name: str) -> Union[None, 'Task']:
        # ret_task: return the matched task too
        for task in tasks:
            if task.file_name() == name:
                return task
        return None

    @staticmethod
    # Given list of tasks and list of names to exclude return excluded task
    def exclude(tasks: List['Task'], names: List[str]) -> List['Task']:
        ret = []
        for task in tasks:
            if task.file_name() not in names:
                ret.append(task)

        return ret

    @staticmethod
    # get incomplete tasks by finish tag
    def incomplete_tasks(tasks, by='all', error_code=None):
        # by: all(return list): total incomplete tasks,
        #     cat(return dict[list]): separate list of tasks by error code, (error code comes from is_job_finished)
        #           error_code: if error code is defined get that category tasks (return list of tasks)
        #     both(return dict): return both all and cat
        #     status(return dict): return count status of each category in a dict

        tasks_incomplete = []
        tasks_incomplete_by_status = defaultdict(list)
        for task in tasks:
            status = pyutils.is_job_finished(file_path=task.out, by='cat')
            if not status['found']:
                tasks_incomplete.append(task)
                tasks_incomplete_by_status[status['cat']].append(task)

        if by == 'all':
            return tasks_incomplete
        elif by == 'cat':
            if error_code is None:
                return tasks_incomplete_by_status
            else:
                return tasks_incomplete_by_status.get(error_code, [])
        elif by == 'both':
            tasks_incomplete_by_status['all'] = tasks_incomplete
            return tasks_incomplete_by_status
        elif by == 'status':
            stat = dict()
            stat['all'] = len(tasks_incomplete)
            for k, v in tasks_incomplete_by_status.items():
                stat[k] = len(v)
            return stat
        else:
            raise ValueError('Invalid by option')


class SlurmTask(Task):

    def __init__(self, task, slurm=None):
        super().__init__(cmd=task.cmd, out=task.out)

        self.slurm = slurm

    def in_slurm(self):
        return True if self.slurm else False

    # update slurm status
    def update(self):
        raise NotImplementedError

    # make sure it is not in the slurm.
    def remove_from_slurm(self):
        # cancel if in slurm. if not in slurm return true
        # on: Slurm Status. Cancel only if in on status
        # maintain status

        # one case not handled: did not check scancel error code if any. And by any how it didn't get cancelled
        Slurm.scancel(opt='id', jobids=self.slurm.id, prod=True)
        return True

    @staticmethod
    # given all task category it
    def tasks_by_status(tasks):
        # return: list of dict by status. All task converted to SlurmTask

        # tasks incomplete by file write
        tasks_incomplete_by_file = Task.incomplete_tasks(tasks=tasks, by='all')

        # get running, pending and rest of the task that is incomplete by file (SlurmTask object)
        slurm_tasks = Slurm.get_slurm_task(tasks=tasks_incomplete_by_file)
        tasks_running, tasks_pending, tasks_incomplete = \
            slurm_tasks[Slurm.RUNNING], slurm_tasks[Slurm.PENDING], slurm_tasks['NOT_IN_SLURM']

        d_task = defaultdict(list, {
            'incomplete': tasks_incomplete,  # by checking file and not running or pending in slurm
            'pending': tasks_pending,
            'running': tasks_running,
            'incomplete_by_file': tasks_incomplete + tasks_pending + tasks_running
            # all task including running and pending
        })

        # get task by file checking category
        incomplete_file_type = Task.incomplete_tasks(tasks=tasks_incomplete, by='cat')

        # merge the file and d_task
        for k, vlist in incomplete_file_type.items():
            if k in d_task:
                raise ValueError('Key supposed to be unique and not exist.')
            d_task[k] = [SlurmTask(task=task) for task in vlist]  # converting to SlurmTask object

        return d_task


# how to launch tasks [all, test, file]
def tasks_launch_action_router(all_tasks, no_resource: int):
    # check file name to submit new jobs
    def callback_file(*args, **kwargs):
        # all task
        tasks = kwargs['tasks']
        no_resource = kwargs['no_resource']

        def cb_get_task_to_submit(*args, **kwargs):
            tasks = kwargs['tasks']
            submit_tasks = []
            for task in tasks:
                try:
                    if task.in_slurm():
                        if task.remove_from_slurm():
                            submit_tasks.append(task)
                    else:
                        submit_tasks.append(task)
                except AttributeError as e:
                    # task is not a SlurmTask. Thus can add directly for submit task
                    submit_tasks.append(task)

            return submit_tasks

        tasks_by_status = SlurmTask.tasks_by_status(tasks=tasks)  # tasks_by_status

        # regenerate ts to conveniently use following code
        ts = deepcopy(tasks_by_status)
        ts['incomplete_status'] = Task.incomplete_tasks(tasks=ts['incomplete'], by='status')
        ts['pending'] = sorted(ts['pending'], key=lambda a: a.slurm.id)  # sorting pending to take last job to cancel

        # when cancelling pending will only cancel on available resources
        # taking at least 1 pending cancel to avoid [-0:]-> return all bug
        no_pending_cancel = max(1, (no_resource - len(ts['incomplete'])))
        pending_on_resource = ts['pending'][-no_pending_cancel:]

        # todo: (remove it) I am keeping it temporarily for debugging purpose. If we need sorting or not
        # pending_on_resource = sorted(ts['pending'], key=lambda a: a.slurm.id)[-no_pending_cancel:]
        print('Pending cancel: ')
        if len(ts['pending']) > 0 and 0 <= -no_pending_cancel-1 < len(ts['pending']):
            print(ts['pending'][0].slurm.id, ts['pending'][-no_pending_cancel-1].slurm.id)
        print(", ".join([str(t.slurm.id) for t in pending_on_resource]))

        tasks_submit = ActionRouter(
            header=f'Status [running: {len(ts["running"])}, pending: {len(ts["pending"])}, incomplete: {ts["incomplete_status"]}]') \
            .add('incomplete', lambda x: x, ts['incomplete'][:no_resource]) \
            .add('incomplete_all', lambda x: x, ts['incomplete']) \
            .add('file_not_exist + pending', cb_get_task_to_submit, tasks=ts['file_not_exist'] + pending_on_resource)\
            .add('incomplete + pending', cb_get_task_to_submit, tasks=ts['incomplete'] + pending_on_resource) \
            .add('incomplete + pending_all', cb_get_task_to_submit, tasks=ts['incomplete'] + ts['pending']) \
            .add('all by file', cb_get_task_to_submit, tasks=ts['incomplete_by_file'])\
            .add('error_in_file', lambda x: x, ts['error_in_file'])\
            .ask().ret()

        return tasks_submit

    # Option: test, all, cache, file -> ')
    tasks_submit = ActionRouter(header='What type of generator to run?') \
        .add('all', lambda x: x, all_tasks) \
        .add('test', lambda x: random.sample(x, min(len(x), 5)), all_tasks) \
        .add('file', callback_file, tasks=all_tasks, no_resource=no_resource) \
        .ask().ret()

    return tasks_submit


# given kwargs dict generate job name and arguments for cmd
def gen_job_name(kwargs, data_dir, batch_path_suffix=None, add_batch_path=True):
    # data_dir: dir to save data. data_dir as added with batch_path
    # batch_path: if given will be that one. Else default one

    job_name, cargs = '', ''
    # generate job_name and cargs
    for k, v in kwargs.items():
        # generate job name
        if not isinstance(v, str) or (
                isinstance(v, str) and len(v) < 30):  # avoid large key value pair, such as prg_path.
            # taking 1 characer for key name. if 1 char match then take more char untill don't match
            # for kidx in range(1, len(k)):
            #     kname = k[:kidx].replace('_', '')
            #     if kname not in job_name: break

            # take first char divide by underscore
            kname = ''
            for kp in k.split('_'):
                if len(kp) > 0: kname += kp[0]
            kv = f'{kname}-{v}'
            job_name += kv if job_name == '' else f'_{kv}'  # (key, val) is separated by underscore
        # generate cmd arguments
        from libpy.Experiment import ExpParam
        # discard if param name is invalid. e.g. want to add in file name but not in param list
        if not ExpParam.invalid_param(name=k):
            cargs = f'{cargs} --{k} {v}'

    # adding batch_path from job name if no batch_path is given
    batch_path = job_name if batch_path_suffix is None else batch_path_suffix
    batch_path = os.path.join(data_dir, batch_path)  # construct full path

    if add_batch_path:
        cargs = f'{cargs} --batch_path {batch_path}'

    if len(job_name) > 210:
        raise ValueError(f'File name may exceed 255 characters. Checking done from create_task. e.g. {job_name}')

    return job_name, cargs


class JobLauncher:
    def __init__(self, task_gen, tasks_each_launch, no_cpu_per_task, time, mem, sbatch_extra_cmd='',
                 submission_check=False):
        '''
        task_gen: is a function that returns list of Task objects. Task object has two parts.
            cmd (string): command that need to be executed. Generally it is the executable with full path. ex. /home/shanto/exe_search
            out (string): where the output will go after executing cmd. It is also with full path. 
                          - Make sure out dir exists beforehand. 
                          - Don't need to use [.out] at the end. [.out] is added with the specific job launcer. ex. /home/shanto/data/here_go_data
        sbatch_extra_cmd: is the cmd that need to place in between sbatch header and executable. Normally it is used to load library or add path
                          - make sure all the commands are with new line including last line 
        tasks_each_launch: usually it is 1 for palab and n for terra cluster
        submission_check: if submission check is true it'll print the submission job without actually submitting. It is for debug purpose
        '''
        self.task_gen = task_gen

        self.no_tasks = tasks_each_launch
        self.no_cpu_per_task = no_cpu_per_task
        self.time = time
        self.mem = mem

        # temporary job directory to put jobs
        self.job_dir = f'{os.getcwd()}/.job'
        pyutils.mkdir_p(self.job_dir)

        self.job_file_name = 'tmp.job'  # file that submit task_file

        self.sbatch_extra_cmd = sbatch_extra_cmd

        self.submission_check = submission_check  # test submission flag

    def sbatch_script(self, header, file):
        raise NotImplementedError

    def launch(self):
        raise NotImplementedError

    def confirm_launch(self, tasks):
        # show details
        def callback_show(*args, **kwargs):
            for task in kwargs['tasks']:
                print(task.out)
            return {"type": "recall"}

        # confirming task submission and return
        ActionRouter(header=f'Total tasks: {len(tasks)}', default_act_use=['abort', 'continue'])\
            .add('show details', callback_show, tasks=tasks)\
            .add('show details (top 5)', callback_show, tasks=tasks[:5])\
            .ask()


class TamuLauncher(JobLauncher):
    '''
    This module uses tamulauncher with slurm batch submission.
    '''

    def __init__(self, task_gen, acc_id=122818929441, tasks_each_launch=14, no_cpu_per_task=1, ntasks_per_node=14,
                 time='00:40:00', mem='50000M', job_name='job', sbatch_extra_cmd='', submission_check=False):
        '''
        :summary
        Let's assume we have in total 1000 tasks to submit. Each task needs 40 minutes time, 100M of memory and 2 cpu. Now assume we want to submit 100 task in each job. Thus, for each job submission
        this module automatically divide the task into different job and submit them. For the above scenario, each of the job will be submitted with 100 task requesting 2*100 cpus, time 40 minutes.
        It'll request only arg memory for each of the job. So for 100 tasks in each job give 100*100M = 10000M in the mem parameter.

        :param
        task_gen: is a function that return list of Task
        acc_id: number of the account
        tasks_each_launch: how many task are in each job submission (previously used 42)
        no_cpu_per_task: cpu allocated for each task
        ntasks_per_node:
        time: time to run all tasks in a job
        mem: memory needed for a job
        sbatch_extra_cmd: extra command to add in batch. e.g. adding PATH or set any value. New line must be provided for each command.

        see https://support.ceci-hpc.be/doc/_contents/SubmittingJobs/SlurmFAQ.html (Q05) for better understanding of allocation
        '''
        super().__init__(task_gen, tasks_each_launch, no_cpu_per_task, time, mem, sbatch_extra_cmd,
                         submission_check=submission_check)

        self.job_name = job_name
        self.acc_id = acc_id
        self.ntasks_per_node = ntasks_per_node

        self.task_file_name = 'tasks'  # file that contain all commands to run the exe

    def sbatch_header(self, job_name='job'):
        if job_name == 'job': job_name = self.job_name  # if nothing is passed use default job_name
        header = (
            f'#!/bin/bash\n'
            f'#SBATCH --export=NONE\n'
            f'#SBATCH --get-user-env=L\n'
            f'#SBATCH --job-name={job_name}\n'
            f'#SBATCH --output={self.job_dir}/{self.job_name}.%j\n'
            f'#SBATCH --time={self.time}\n'
            f'#SBATCH --ntasks={self.no_tasks}\n'
            f'#SBATCH --ntasks-per-node={self.ntasks_per_node}\n'
            f'#SBATCH --cpus-per-task={self.no_cpu_per_task}\n'
            f'#SBATCH --mem={self.mem}\n'
            f'#SBATCH --account={self.acc_id}\n'
        )
        return header

    def sbatch_script(self, header, file):
        script = (
            f'{header}\n'
            f'tamulauncher {file}\n'
        )
        return script

    def _get_tasks_file(self, tasks):

        tasks_job = ''
        for task in tasks:
            tasks_job += f'{task.cmd} 2> {task.out}.err 1> {task.out}.out\n'

        unique_file_name = f'{self.task_file_name}_{uuid.uuid4()}'
        tasks_file = os.path.join(self.job_dir, unique_file_name)
        with open(tasks_file, 'w') as fh:
            fh.writelines(tasks_job)
        return tasks_file

    def check_launcher_cache(self):
        tamulauncher_cache = '.tamulauncher-log'
        pyutils.dir_choice(tamulauncher_cache)

    def launch(self):

        tasks = self.task_gen()
        self.check_launcher_cache()

        # number of submitted jobs will be
        total_jobs = int(len(tasks) / self.no_tasks)
        if len(tasks) % self.no_tasks > 0: total_jobs += 1
        print(f'Total number of sbatch job: {total_jobs}')

        for job_id, i in enumerate(range(0, len(tasks), self.no_tasks)):
            tasks_job = self._get_tasks_file(tasks[i:i + self.no_tasks])

            # construct header
            header = self.sbatch_header(job_name=f'{self.job_name}_{job_id}') + self.sbatch_extra_cmd
            # construct script
            script = self.sbatch_script(header=header, file=tasks_job)

            u_job_file_name = f'{self.job_file_name}_{job_id}_{uuid.uuid4()}'
            job_file = os.path.join(self.job_dir, u_job_file_name)
            with open(job_file, 'w') as fh:
                fh.writelines(script)
            print(f'sbatch {job_file}')
            if not self.submission_check:
                success = Slurm.sbatch(file=job_file, prod=True, verbose=True)
                return success


class SlurmLauncher(JobLauncher):
    '''
    This module uses slurm batch submission.
    '''

    def __init__(self, task_gen, tasks_each_launch, no_cpu_per_task, time, mem, sbatch_extra_cmd='',
                 submission_check=False):
        '''
        Caution: time and mem doesn't have any effect here.

        Make sure all the directories are already exist
        task_gen is a function that return list of Task
        no_exclude_node: how many node not to use. If you doesn't want any node to exclude pass 0 here. Otherwise change node name to match your cluster.
        '''
        super().__init__(task_gen, tasks_each_launch, no_cpu_per_task, time, mem, sbatch_extra_cmd,
                         submission_check=submission_check)

    def sbatch_header(self, job_name, out):
        header = (
            f'#!/bin/bash\n'
            f'#SBATCH --job-name={job_name}\n'
            f'#SBATCH --output={out}.out\n'
            f'#SBATCH --error={out}.err\n'
            f'#SBATCH --cpus-per-task={self.no_cpu_per_task}\n'
        )
        return header

    def sbatch_script(self, header, cmd):
        # merge header and command together
        script = (
            f'{header}\n'
            f'{self.sbatch_extra_cmd}\n'
            f'{cmd}\n'
        )
        return script

    def submit_job(self, task, job_script, verbose=False):
        # creating job file with unique file name
        out_file_name = os.path.basename(task.out)
        u_job_file_name = f'{self.job_file_name}_{out_file_name}_{uuid.uuid4()}'
        job_file = os.path.join(self.job_dir, u_job_file_name)
        with open(job_file, 'w') as fh:
            fh.write(job_script)

        # run the script
        if verbose:
            print(f'sbatch {job_file}')
        if not self.submission_check:
            success = Slurm.sbatch(file=job_file, verbose=verbose, prod=True)
            return success

    # submit all jobs
    def submit_all_jobs(self, jobs, verbose=1):
        # jobs contain: [task, job_script]
        # verbose: 0: no verbose 1: print only failed 2: details

        failed = []
        for task, job_script in tqdm(jobs, desc='Slurm job submission'):
            success = self.submit_job(task=task, job_script=job_script, verbose=(verbose==2))
            if not success:
                failed.append((task, job_script))

        if verbose != 0:
            if len(failed) > 0:
                pyutils.errprint(f'Total filed job submission: {len(failed)}/{len(jobs)}', time_stamp=False)



    def launch(self):
        raise NotImplementedError

    # check resource and sort out tasks to be submitted
    def pre_launch(self):
        # getting free resources
        free_resource = Slurm.get_resource(no_cpu=self.no_cpu_per_task)
        print(f'Resources available: {len(free_resource)}')

        # generating all tasks
        tasks = self.task_gen()
        tasks = tasks_launch_action_router(tasks, len(free_resource))

        self.confirm_launch(tasks=tasks)

        if len(free_resource) == 0:
            free_resource = ['all']
        
        return {
            'tasks': tasks,
            'free_resource': free_resource
        }


class PAlabLauncher(SlurmLauncher):
    '''
    This module uses slurm batch submission.
    '''

    def __init__(self, task_gen, tasks_each_launch=1, no_cpu_per_task=1, time='9999:40:00', mem='2000M',
                 sbatch_extra_cmd='', no_exclude_node=1, submission_check=False):
        '''
        Caution: time and mem doesn't have any effect here.

        Make sure all the directories are already exist
        task_gen is a function that return list of Task
        no_exclude_node: how many node not to use. If you doesn't want any node to exclude pass 0 here. Otherwise change node name to match your cluster.
        '''
        # adding exclude node command
        self.no_exclude_node = no_exclude_node
        super().__init__(task_gen, tasks_each_launch, no_cpu_per_task, time, mem, sbatch_extra_cmd=sbatch_extra_cmd,
                         submission_check=submission_check)

    def _cluster_specific_header(self, node_name, no_exclude_node):
        # header to exclude node
        header = ''
        if no_exclude_node > 0:
            exclude = f'01-0{no_exclude_node}'
            header = (
                f'#SBATCH --exclude={node_name}[{exclude}]\n'
            )
        return header

    def launch(self):
        # generating all tasks
        launch_var = self.pre_launch()
        tasks, resource = launch_var['tasks'], launch_var['free_resource']

        # process jobs to submit
        jobs_to_submit = []
        for task in tasks:
            # getting header with job_name, out and err file name
            job_name = os.path.basename(task.out)  # take the output file name as job name as output file name is unique

            extra_cluster_specific_cmd = self._cluster_specific_header(node_name='node',
                                                                       no_exclude_node=self.no_exclude_node)
            header = self.sbatch_header(job_name, task.out) + extra_cluster_specific_cmd
            # command to execute
            cmd = task.cmd

            # make script with header and command
            job_script = self.sbatch_script(header, cmd)

            # jobs to submit
            jobs_to_submit.append((task, job_script))

        # submit jobs
        self.submit_all_jobs(jobs=jobs_to_submit, verbose=1)


class AtlasLauncher(SlurmLauncher):
    '''
    This module uses slurm batch submission.
    '''

    def __init__(self, task_gen, tasks_each_launch=1, no_cpu_per_task=1, time='9999:40:00', mem='2000M',
                 sbatch_extra_cmd='', submission_check=False, ):
        '''
        Caution: time and mem doesn't have any effect here.

        Make sure all the directories are already exist
        task_gen is a function that return list of Task
        no_exclude_node: how many node not to use. If you doesn't want any node to exclude pass 0 here. Otherwise change node name to match your cluster.
        '''
        super().__init__(task_gen, tasks_each_launch, no_cpu_per_task, time, mem, sbatch_extra_cmd=sbatch_extra_cmd,
                         submission_check=submission_check)

    def _add_partition(self, header, partition_name):
        header += f'#SBATCH --partition={partition_name}\n'
        return header

    def launch(self):

        launch_var = self.pre_launch()
        tasks, resource = launch_var['tasks'], launch_var['free_resource']

        use_all = True
        if use_all:
            resource = resource + ['all' for _ in range(len(tasks) - len(resource))]
        else:
            resource = itertools.cycle(resource)

        # process jobs to submit
        jobs_to_submit = []
        for task, partition_name in zip(tasks, resource):
            # getting header with job_name, out and err file name
            job_name = os.path.basename(task.out)  # take the output file name as job name as output file name is unique
            header = self.sbatch_header(job_name, task.out)
            # add partition with header
            header = self._add_partition(header=header, partition_name=partition_name)
            # command to execute
            cmd = task.cmd

            # make script with header and command
            job_script = self.sbatch_script(header=header, cmd=cmd)

            # jobs to submit
            jobs_to_submit.append((task, job_script))

        # submit jobs
        self.submit_all_jobs(jobs=jobs_to_submit, verbose=1)


class TerraGPULauncher(PAlabLauncher):

    def __init__(self, task_gen, acc_id=12281892943, tasks_each_launch=1, no_cpu_per_task=10, no_gpu=1, time='24:00:00',
                 mem='100000M', sbatch_extra_cmd='', no_exclude_node=0, submission_check=False):
        '''
        Make sure all the directories are already exist
        task_gen is a function that return list of Task
        no_exclude_node: how many node not to use. If you doesn't want any node to exclude pass 0 here. Otherwise change node name to match your cluster.
        '''
        self.acc_id = acc_id
        self.no_gpu = no_gpu
        super().__init__(task_gen, tasks_each_launch, no_cpu_per_task, time, mem, sbatch_extra_cmd=sbatch_extra_cmd,
                         no_exclude_node=no_exclude_node, submission_check=submission_check)

    def _cluster_specific_header(self, node_name, no_exclude_node):
        # header to use gpu

        header = (
            f'#SBATCH --export=NONE\n'
            f'#SBATCH --get-user-env=L\n'
            f'#SBATCH --account={self.acc_id}\n'
            f'#SBATCH --time={self.time}\n'
            f'#SBATCH --ntasks={self.no_tasks}\n'
            f'#SBATCH --mem={self.mem}\n'
            f'#SBATCH --gres=gpu:{self.no_gpu}\n'
            f'#SBATCH --partition=gpu\n'
        )
        return header


# cluster launch job
def launch_job(cluster, callback_batch_gen, job_name, no_cpu=1, time='3:00:00', no_exlude_node=1,
               submission_check=False, sbatch_extra_cmd='source activate rl\n',
               acc_id=122818929441, tasks_each_launch=14):
    '''

    :param cluster: name of the cluster
    :param callback_batch_gen: a callback function that return a list of Task object
    :param job_name: any string to show the job name in cluster
    :param no_cpu: how many cpu to use per task
    :param time:
    :param no_exlude_node: for palab cluster exlude some node to not submit jobs
    :param submission_check: if submission_check is false it will only create the sbatch file but will not submit them automatically
    :param sbatch_extra_cmd: any extra command need to be done before executing original program
    :param acc_id: acc_id is mainly for tamu hprc
    :param tasks_each_launch: it is required for tamulauncher. how many tasks will be launched each time
    :return:
    '''
    # choose cluster
    if cluster == 'palab':
        server = PAlabLauncher(callback_batch_gen, sbatch_extra_cmd=sbatch_extra_cmd, no_cpu_per_task=no_cpu,
                               no_exclude_node=no_exlude_node, submission_check=submission_check)
    elif cluster == 'atlas':
        server = AtlasLauncher(callback_batch_gen, sbatch_extra_cmd=sbatch_extra_cmd, no_cpu_per_task=no_cpu,
                               submission_check=submission_check)
    elif cluster == 'tamulauncher':
        import router  # as router may not be present in every project importing here
        sbatch_extra_cmd = f'source {os.path.join(router.project_root, "TerraModuleCPU.sh")}\n' \
                           f'unset I_MPI_PMI_LIBRARY'
        # don't use tasks_each_launch in tamulauncher. It has a bug that doesn't follow tasks-per-node hence request large SUs
        server = TamuLauncher(callback_batch_gen, job_name=job_name, acc_id=acc_id, sbatch_extra_cmd=sbatch_extra_cmd,
                              time=time, submission_check=submission_check, tasks_each_launch=tasks_each_launch)
    elif cluster == 'terragpu':
        import router  # as router may not be present in every project importing here
        sbatch_extra_cmd = f'source {os.path.join(router.project_root, "TerraModule.sh")}'
        server = TerraGPULauncher(callback_batch_gen, acc_id=acc_id, sbatch_extra_cmd=sbatch_extra_cmd, time=time,
                                  submission_check=submission_check)
    else:
        raise ValueError('Invalid cluster name!!')

    # launch jobs
    server.launch()
