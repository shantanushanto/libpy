import os
import uuid
import time
import dill
import sys

from libpy import pyutils


class Task():
    def __init__(self, cmd, out):
        self.cmd = cmd
        self.out = out

    @staticmethod
    def cache_tasks(tasks, dir):
        # cache list of tasks.
        task_cache_file = os.path.join(dir, f'.tasks_{time.time_ns()}_n-task-{len(tasks)}_{time.ctime().replace(" ", "-")}.tasks')
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
                err_path = f'{task.out}.err'
                if os.path.isfile(err_path): # check if file exist but finish tag doesn't exist
                    with open(err_path, 'r') as f:
                        # check finish tag
                        if finish_tag not in f.read():
                            incomplete_tasks.append(task)
                else: # err file doesn't exist. Need to rerun
                    incomplete_tasks.append(task)

            return incomplete_tasks

        # previously submitted task
        submitted_tasks = get_tasks()
        # find incomplete task
        incomplete = get_incomplete_task(submitted_tasks)
        if verbose: sys.stderr.write(f'Total incomplte task: {len(incomplete)}\n')
        return incomplete

class JobLauncher():
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
    def __init__(self, task_gen, acc_id = 122818929441, tasks_each_launch = 42, no_cpu_per_task = 1, ntasks_per_node = 14, time = '00:40:00', mem = '50000M', job_name = 'job', sbatch_extra_cmd = '', submission_check=False):
        '''
        :summary
        Let's assume we have in total 1000 tasks to submit. Each task needs 40 minutes time, 100M of memory and 2 cpu. Now assume we want to submit 100 task in each job. Thus, for each job submission
        this module automatically divide the task into different job and submit them. For the above scenario, each of the job will be submitted with 100 task requesting 2*100 cpus, time 40 minutes.
        It'll request only arg memory for each of the job. So for 100 tasks in each job give 100*100M = 10000M in the mem parameter.

        :param
        task_gen: is a function that return list of Task
        acc_id: number of the account
        tasks_each_launch: how many task are in each job submission
        no_cpu_per_task: cpu allocated for each task
        ntasks_per_node:
        time: time to run all tasks in a job
        mem: memory needed for a job
        sbatch_extra_cmd: extra command to add in batch. e.g. adding PATH or set any value. New line must be provided for each command.
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

    def __init__(self, task_gen, tasks_each_launch=1, no_cpu_per_task=1, time='9999:40:00', mem='2000M', sbatch_extra_cmd='', submission_check=False, atlas_ratio=4):
        '''
        Caution: time and mem doesn't have any effect here.

        Make sure all the directories are already exist
        task_gen is a function that return list of Task
        no_exclude_node: how many node not to use. If you doesn't want any node to exclude pass 0 here. Otherwise change node name to match your cluster.
        '''
        super().__init__(task_gen, tasks_each_launch, no_cpu_per_task, time, mem, sbatch_extra_cmd=sbatch_extra_cmd, submission_check=submission_check)
        self.atlas_ratio = atlas_ratio

    def _add_partition(self, idx, header):
        # atlas has two partitions. Distribute as all: 3, 12-core: 1
        if idx % self.atlas_ratio == 0:
            header += '#SBATCH --partition=12-core\n'
        return header

    def launch(self):
        # generating all tasks
        tasks = self.task_gen()

        for idx, task in enumerate(tasks):
            # getting header with job_name, out and err file name
            job_name = os.path.basename(task.out)  # take the output file name as job name as output file name is unique
            header = self.sbatch_header(job_name, task.out)
            # add partition with header
            header = self._add_partition(idx=idx, header=header)
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
def launch_job(cluster, callback_batch_gen, job_name, no_cpu=1, time='3:00:00', no_exlude_node=1, atlas_ratio=4, submission_check=False, sbatch_extra_cmd='', acc_id=122818929441):
    sbatch_extra_cmd += "source activate rl\n"
    # choose cluster
    if cluster == 'palab':
        server = PAlabLauncher(callback_batch_gen, sbatch_extra_cmd=sbatch_extra_cmd, no_cpu_per_task=no_cpu, no_exclude_node=no_exlude_node, submission_check=submission_check)
    elif cluster == 'atlas':
        server = AtlasLauncher(callback_batch_gen, sbatch_extra_cmd=sbatch_extra_cmd, no_cpu_per_task=no_cpu, atlas_ratio=atlas_ratio, submission_check=submission_check)
    elif cluster == 'tamulauncher':
        import router  # as router may not be present in every project importing here
        sbatch_extra_cmd = f'source {os.path.join(router.project_root, "TerraModuleCPU.sh")}'
        server = TamuLauncher(callback_batch_gen, job_name=job_name, acc_id=acc_id, sbatch_extra_cmd=sbatch_extra_cmd, time=time, submission_check=submission_check)
    elif cluster == 'terragpu':
        import router  # as router may not be present in every project importing here
        sbatch_extra_cmd = f'source {os.path.join(router.project_root, "TerraModule.sh")}'
        server = TerraGPULauncher(callback_batch_gen, acc_id=acc_id, sbatch_extra_cmd=sbatch_extra_cmd, time=time, submission_check=submission_check)
    else:
        raise ValueError('Invalid cluster name!!')

    # launch jobs
    server.launch()

