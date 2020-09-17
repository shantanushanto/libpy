import os
import uuid
import time
import dill
import sys
import random
import itertools
import subprocess
from typing import List

from libpy import pyutils, commonutils

class Slurm:

    user = 'shanto'

    @staticmethod
    # cancel by status or job_id. job id either in int or string. For multiple job id must be string comma seperated
    def scancel(opt: str, jobids=None, prod=False):
        # opt : pending, all, id
        scancel_pre = f'scancel -u {Slurm.user}'
        if opt == 'all':
            cmd = f'{scancel_pre}'  # scancel -u shanto
        elif opt == 'pending':
            cmd = f'{scancel_pre} --state PENDING'  # scancel -u shanto --state PENDING
        elif opt == 'id':
            # scancel your_job-id1, your_job-id2, your_jobiid3
            if jobids == None:
                pass
            elif type(jobids) == int or type(jobids) == str:
                cmd = f'{scancel_pre} {jobids}'
            elif type(jobids) == list:
                p = ", ".join(jobids)
                cmd = f'{scancel_pre} {p}'
        else:
            raise ValueError('Invalid type')

        if prod:
            os.system(cmd)

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


        pyutils.ActionRouter(header=f'Total job to be canceled: {len(ids)}')\
            .add('show_details', callback_show_details, ids=ids, names=names)\
            .add('cancel_direct', callback_cancel, ids=ids)\
            .ask()

    # get job that is in the sbatch running or pending
    @staticmethod
    def get_job_in_sbatch():
        
        # return job in a list. job is dict of {id, status, name}
        def parse(lines):
            lines = [line.strip().strip('"') for line in lines.split('\n') if len(line.strip().strip('"')) > 0]

            jobs = []
            for line in lines:
                u = line.split(' ')
                job = {'id': u[0],
                       'status': u[1],
                       'name': u[2]
                       }
                jobs.append(job)
            return jobs
        cmd = ['squeue', '-u', f'{Slurm.user}', '--format="%i %T %j"', '--noheader']  # id, status, job_fullname
        try:
            result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
            if 'error' in result.stderr:
                raise ValueError(f'SlurmError: {result.stderr}')
            jobs = parse(lines=result.stdout)
            return jobs
        except:
            pyutils.errprint(f"SlurmError: in getting Job id [{' '.join(cmd)}]", time_stamp=False)
            return None


class Task:
    def __init__(self, cmd, out):
        self.cmd = cmd
        self.out = out  # full path of out file

    # get only file name
    def file_name(self):
        return os.path.basename(self.out)

    @staticmethod
    def cache_tasks(tasks, dir):
        # cache list of tasks.
        task_cache_file = os.path.join(dir, f'.tasks_{time.time()}_n-task-{len(tasks)}_{time.ctime().replace(" ", "-")}.tasks')
        with open(task_cache_file, 'wb') as file:
            dill.dump(tasks, file)

    @staticmethod
    def incomplete_tasks_from_cache(dir, file_name=None, finish_tag=pyutils.tag_job_finished_successfully, verbose=True):
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
    def find(tasks: List['Task'], name: str) -> bool:
        for task in tasks:
            if task.file_name() == name:
                return True
        return False

    @staticmethod
    # Given list of tasks and list of names to exclude return excluded task
    def exclude(tasks: List['Task'], names: List[str]) -> List['Task']:
        ret = []
        for task in tasks:
            if task.file_name() not in names:
                ret.append(task)

        return ret


class TaskGenerator:

    def __init__(self, batch_generator, data_dir):
        self.batch_generator = batch_generator
        self.data_dir = data_dir

        self.tasks = None  # tasks set for caching

    def _incomplete_job(self, tasks_incomplete_by_file: List[Task], verbose=True) -> List[Task]:
        # Works as: finding jobs in cluster, depending on the options, regenerate incomplete task to submit
        # get job details running in cluster
        job_in_slurm = Slurm.get_job_in_sbatch()
        
        # no job in cluster. thus no need to ask for option
        if job_in_slurm ==  None or len(job_in_slurm) == 0:
            print('No jobs run in cluster')
            return tasks_incomplete_by_file

        # calculate job number in different state
        no_job = {'incomplete_by_file': len(tasks_incomplete_by_file), 'running': 0, 'pending': 0, 'incomplete': 0}

        # job name by their status
        name_pending, name_running = [], []

        # calculate total running and pending jobs and store their name
        for job in job_in_slurm:
            if job['status'] == 'PENDING':
                no_job['pending'] += 1
                name_pending.append(job['name'])
            elif job['status'] == 'RUNNING':
                no_job['running'] += 1
                name_running.append(job['name'])

        # number of incomplete job
        no_job['incomplete'] = max(0, (no_job['incomplete_by_file'] - no_job['running'] - no_job['pending']))

        # incomplete_tasks_by_file - pending - running
        def callback_only_incomplete(*args, **kwargs):
            tasks = Task.exclude(kwargs['tasks'], kwargs['exclude'])
            return tasks

        # incomplete_tasks_by_file - running. cancel pending and return incomplete including (pending jobs)
        def callback_rebalance(*args, **kwargs):
            # cancel pending jobs and subtract only running jobs from incomplete
            Slurm.scancel(opt='pending', prod=True)
            tasks = Task.exclude(kwargs['tasks'], kwargs['exclude'])
            return tasks

        # cancel all and return all tasks_incomplete_by_file
        def callback_all_incomplete(*args, **kwargs):
            # cancel all jobs and rerun from file
            Slurm.scancel(opt='all', prod=True)
            return kwargs['tasks']
            pass

        header = f'Job status [running: {no_job["running"]}, pending: {no_job["pending"]}, ' \
                 f'incomplete: {no_job["incomplete"]}]'

        tasks_incomplete = pyutils.ActionRouter(header=header)\
            .add('only_incomplete', callback_only_incomplete, tasks=tasks_incomplete_by_file, exclude=name_pending+name_running)\
            .add('rebalance [incom+pend]', callback_rebalance, tasks=tasks_incomplete_by_file, exclude=name_pending)\
            .add('all_incomplete', callback_all_incomplete, tasks=tasks_incomplete_by_file)\
            .ask().ret()

        return tasks_incomplete

    # create testing run tasks for batch submission
    def _test_batch_generator(self):
        tasks = self.batch_generator()
        test_task = random.sample(tasks, min(len(tasks), 5))
        return test_task

    # submit jobs from recent cache
    def _incomplete_batch_generator(self):
        tasks = Task.incomplete_tasks_from_cache(self.data_dir)
        return tasks

    # check file name to submit new jobs
    def _file_check_batch_generator(self, **kwargs):
        tasks = self.batch_generator()
        tasks_incomplete = []
        for task in tasks:
            if not pyutils.is_job_finished(file_path=task.out):
                tasks_incomplete.append(task)

        # exclude tasks that is not complete by file but running in the cluster
        tasks_incomplete = self._incomplete_job(tasks_incomplete_by_file=tasks_incomplete)

        # only need the tasks
        if 'only_ret_tasks' in kwargs and kwargs['only_ret_tasks'] == True:
            return tasks_incomplete

        # show detail files if wanted
        inp = input(f'#incomplete tasks: {len(tasks_incomplete)}. Show details (n will submit immediately)? [y/n] -> ')
        if inp == 'y':
            for task in tasks_incomplete:
                print(task.out)
            # grant permission to submit
            inp = input('Continue to submit? [y/n] -> ')
            if inp != 'y':
                exit(0)
        return tasks_incomplete

    def _all_batch_generator(self, **kwargs):
        tasks_incomplete = self.batch_generator()

        # only need the tasks
        if 'only_ret_tasks' in kwargs and kwargs['only_ret_tasks'] == True:
            return tasks_incomplete

        inp = input(f'#Total tasks: {len(tasks_incomplete)}. Submit?  [y/n] -> ')
        if inp != 'y':
            exit(0)
        return tasks_incomplete

    # select batchgen type
    def _callback_batch_gen_options(self):
        batch_gen_type = input('What type of generator to run? Option: test, all, cache, file -> ')
        if batch_gen_type == 'test':
            return self._test_batch_generator
        elif batch_gen_type == 'all':
            return self._all_batch_generator
        elif batch_gen_type == 'cache':
            return self._incomplete_batch_generator
        elif batch_gen_type == 'file':
            return self._file_check_batch_generator
        else:
            print('Wrong input!')
            return self._callback_batch_gen_options()

    def get_callback_batch_gen(self):

        callback_batch_gen = self._callback_batch_gen_options()
        # creating task cache
        # disabling task cache creation
        # Task.cache_tasks(callback_batch_gen(only_ret_tasks=True), self.data_dir)
        return callback_batch_gen


# given kwargs dict generate job name and arguments for cmd
def gen_job_name(kwargs, data_dir, batch_path_suffix=None):
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
        cargs = f'{cargs} --{k} {v}'

    # adding batch_path from job name if no batch_path is given
    batch_path = job_name if batch_path_suffix is None else batch_path_suffix
    batch_path = os.path.join(data_dir, batch_path)  # construct full path
    cargs = f'{cargs} --batch_path {batch_path}'
    if len(job_name) > 210:
        raise ValueError(f'File name may exceed 255 characters. Checking done from create_task. e.g. {job_name}')

    return job_name, cargs


class JobLauncher:
    def __init__(self, task_gen, tasks_each_launch, no_cpu_per_task, time, mem, sbatch_extra_cmd='', submission_check=False):
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


class TamuLauncher(JobLauncher):
    '''
    This module uses tamulauncher with slurm batch submission.
    '''
    def __init__(self, task_gen, acc_id = 122818929441, tasks_each_launch = 14, no_cpu_per_task = 1, ntasks_per_node = 14, time = '00:40:00', mem = '50000M', job_name = 'job', sbatch_extra_cmd = '', submission_check=False):
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
        super().__init__(task_gen, tasks_each_launch, no_cpu_per_task, time, mem, sbatch_extra_cmd, submission_check=submission_check)

        self.job_name = job_name 
        self.acc_id = acc_id 
        self.ntasks_per_node = ntasks_per_node

        self.task_file_name = 'tasks' # file that contain all commands to run the exe

    def sbatch_header(self, job_name='job'):
        if job_name == 'job': job_name=self.job_name  # if nothing is passed use default job_name
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
        total_jobs = int(len(tasks)/self.no_tasks)
        if len(tasks)%self.no_tasks > 0: total_jobs += 1
        print(f'Total number of sbatch job: {total_jobs}')

        for job_id, i in enumerate(range(0, len(tasks), self.no_tasks)):
            tasks_job = self._get_tasks_file(tasks[i:i+self.no_tasks])

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
                os.system(f'sbatch {job_file}')


class SlurmLauncher(JobLauncher):
    '''
    This module uses slurm batch submission.
    '''
    def __init__(self, task_gen, tasks_each_launch, no_cpu_per_task, time, mem, sbatch_extra_cmd='', submission_check=False):
        '''
        Caution: time and mem doesn't have any effect here.

        Make sure all the directories are already exist
        task_gen is a function that return list of Task
        no_exclude_node: how many node not to use. If you doesn't want any node to exclude pass 0 here. Otherwise change node name to match your cluster.
        '''
        super().__init__(task_gen, tasks_each_launch, no_cpu_per_task, time, mem, sbatch_extra_cmd, submission_check=submission_check)

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

    def submit_job(self, task, job_script):
        # creating job file with unique file name
        out_file_name = os.path.basename(task.out)
        u_job_file_name = f'{self.job_file_name}_{out_file_name}_{uuid.uuid4()}'
        job_file = os.path.join(self.job_dir, u_job_file_name)
        with open(job_file, 'w') as fh:
            fh.write(job_script)

        # run the script
        print(f'sbatch {job_file}')
        if not self.submission_check:
            os.system(f'sbatch {job_file}')

    def launch(self):
        raise NotImplementedError


class PAlabLauncher(SlurmLauncher):
    '''
    This module uses slurm batch submission.
    '''
    def __init__(self, task_gen, tasks_each_launch = 1, no_cpu_per_task = 1, time = '9999:40:00', mem = '2000M', sbatch_extra_cmd = '', no_exclude_node = 1, submission_check=False):
        '''
        Caution: time and mem doesn't have any effect here.

        Make sure all the directories are already exist
        task_gen is a function that return list of Task
        no_exclude_node: how many node not to use. If you doesn't want any node to exclude pass 0 here. Otherwise change node name to match your cluster.
        '''
        # adding exclude node command
        self.no_exclude_node = no_exclude_node
        super().__init__(task_gen, tasks_each_launch, no_cpu_per_task, time, mem, sbatch_extra_cmd=sbatch_extra_cmd, submission_check=submission_check)

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
        tasks = self.task_gen()

        for task in tasks:
            # getting header with job_name, out and err file name
            job_name = os.path.basename(task.out) # take the output file name as job name as output file name is unique

            extra_cluster_specific_cmd = self._cluster_specific_header(node_name='node', no_exclude_node=self.no_exclude_node)
            header = self.sbatch_header(job_name, task.out) + extra_cluster_specific_cmd
            # command to execute
            cmd = task.cmd

            # make script with header and command
            job_script = self.sbatch_script(header, cmd)

            # submitting job
            self.submit_job(task=task, job_script=job_script)


class AtlasLauncher(SlurmLauncher):
    '''
    This module uses slurm batch submission.
    '''

    def __init__(self, task_gen, tasks_each_launch=1, no_cpu_per_task=1, time='9999:40:00', mem='2000M', sbatch_extra_cmd='', submission_check=False,):
        '''
        Caution: time and mem doesn't have any effect here.

        Make sure all the directories are already exist
        task_gen is a function that return list of Task
        no_exclude_node: how many node not to use. If you doesn't want any node to exclude pass 0 here. Otherwise change node name to match your cluster.
        '''
        super().__init__(task_gen, tasks_each_launch, no_cpu_per_task, time, mem, sbatch_extra_cmd=sbatch_extra_cmd, submission_check=submission_check)

    def _add_partition(self, header, partition_name):
        header += f'#SBATCH --partition={partition_name}\n'
        return header

        '''
        # atlas has two partitions. Distribute as all: 3, 12-core: 1
        if self.atlas_ratio == -1:  # use all core
            pass
        elif self.atlas_ratio == -2:  # use all 12-core node
            header += '#SBATCH --partition=12-core\n'
        elif (idx+1) % self.atlas_ratio == 0:
            header += '#SBATCH --partition=12-core\n'
        return header
        '''

    # get free resources by partition name in a list
    def _get_resource(self, no_cpu):

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
            free_blocks = free_cpu_block(blocks=blocks, no_cpu=no_cpu)
            return free_blocks
        except:
            pyutils.errprint(f"SlurmError: in getting cluster available resourcess [{' '.join(cmd)}]", time_stamp=False)
            return ['all', '12-core', 'bigmo']

    def launch(self):

        # getting free resources
        free_resource = self._get_resource(no_cpu=self.no_cpu_per_task)
        # print(f'Resources available: {len(free_resource)}')

        # generating all tasks
        tasks = self.task_gen()

        pyutils.ActionRouter(header=f'Resources need: {len(tasks)} available: {len(free_resource)}',
                             default_act_use=['abort', 'continue']).ask()

        use_all = True
        if use_all:
            free_resource = free_resource + ['all' for _ in range(len(tasks)-len(free_resource))]
        else:
            free_resource = itertools.cycle(free_resource)

        for task, partition_name in zip(tasks, free_resource):
            # getting header with job_name, out and err file name
            job_name = os.path.basename(task.out)  # take the output file name as job name as output file name is unique
            header = self.sbatch_header(job_name, task.out)
            # add partition with header
            header = self._add_partition(header=header, partition_name=partition_name)
            # command to execute
            cmd = task.cmd

            # make script with header and command
            job_script = self.sbatch_script(header=header, cmd=cmd)

            # submitting job
            self.submit_job(task=task, job_script=job_script)


class TerraGPULauncher(PAlabLauncher):

    def __init__(self, task_gen, acc_id=12281892943, tasks_each_launch = 1, no_cpu_per_task = 10, no_gpu=1, time = '24:00:00', mem = '100000M', sbatch_extra_cmd = '', no_exclude_node = 0, submission_check=False):
        '''
        Make sure all the directories are already exist
        task_gen is a function that return list of Task
        no_exclude_node: how many node not to use. If you doesn't want any node to exclude pass 0 here. Otherwise change node name to match your cluster.
        '''
        self.acc_id = acc_id
        self.no_gpu = no_gpu
        super().__init__(task_gen, tasks_each_launch, no_cpu_per_task, time, mem, sbatch_extra_cmd=sbatch_extra_cmd, no_exclude_node=no_exclude_node, submission_check=submission_check)

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
def launch_job(cluster, callback_batch_gen, job_name, no_cpu=1, time='3:00:00', no_exlude_node=1, submission_check=False, sbatch_extra_cmd='source activate rl\n',
               acc_id=122818929441, tasks_each_launch=14):
    # choose cluster
    if cluster == 'palab':
        server = PAlabLauncher(callback_batch_gen, sbatch_extra_cmd=sbatch_extra_cmd, no_cpu_per_task=no_cpu, no_exclude_node=no_exlude_node, submission_check=submission_check)
    elif cluster == 'atlas':
        server = AtlasLauncher(callback_batch_gen, sbatch_extra_cmd=sbatch_extra_cmd, no_cpu_per_task=no_cpu, submission_check=submission_check)
    elif cluster == 'tamulauncher':
        import router  # as router may not be present in every project importing here
        sbatch_extra_cmd = f'source {os.path.join(router.project_root, "TerraModuleCPU.sh")}\n' \
                           f'unset I_MPI_PMI_LIBRARY'
        # don't use tasks_each_launch in tamulauncher. It has a bug that doesn't follow tasks-per-node hence request large SUs
        server = TamuLauncher(callback_batch_gen, job_name=job_name, acc_id=acc_id, sbatch_extra_cmd=sbatch_extra_cmd, time=time, submission_check=submission_check, tasks_each_launch=tasks_each_launch)
    elif cluster == 'terragpu':
        import router  # as router may not be present in every project importing here
        sbatch_extra_cmd = f'source {os.path.join(router.project_root, "TerraModule.sh")}'
        server = TerraGPULauncher(callback_batch_gen, acc_id=acc_id, sbatch_extra_cmd=sbatch_extra_cmd, time=time, submission_check=submission_check)
    else:
        raise ValueError('Invalid cluster name!!')

    # launch jobs
    server.launch()

