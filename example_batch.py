import argparse
import os
import time

from JobLauncher import Task, TamuLauncher, PAlabLauncher
from pyutils import dir_choice

# Submit jobs for example_program
def run_example_program(cluster):
    # dir to save data in
    data_dir = 'data'
    data_dir = dir_choice(data_dir)  # dir checking

    # program to run
    exe = 'example_program'

    # create readme file in the folder to easy remember
    with open(os.path.join(data_dir, 'readme'), 'w') as f:
        f.write(f'Date created: {time.time()}'
                f'This directory is created to demonstrate how to use Tamulauncher or Slurm with python script')

    # create single run cmd
    def create_task(number, sleep_time):
        # Job name is used as file name also. Make it unique
        job_name = f'job_name_{number:04}_{sleep_time}' # need to edit
        # Joblauncher create .out and .err two files with out name in the data_dir for each of the run
        out = os.path.join(data_dir, job_name)
        # create program to run with arguments
        cmd = f'python {exe} --number {number} --sleep_time {sleep_time}'
        # creating task
        return Task(cmd, out)

    # create multiple run tasks for batch submission
    def batch_generator():
        tasks = []
        # create all tasks with different parameter combination
        for number in range(10):
            sleep_time = 60 # seconds
            task = create_task(number=number, sleep_time=sleep_time)
            tasks.append(task)
        return tasks

    # create testing run tasks for batch submission. Normally there are thousands of tasks to submit. So it is better to run s small test
    def test_batch_generator():
        tasks = []
        # create all tasks with different parameter combination
        for number in range(2):
            sleep_time = 5  # for testing decreased sleep time
            task = create_task(number=number, sleep_time=sleep_time)
            tasks.append(task)
        return tasks

    # sometime jobs might fail or some task doesn't produce result successfully. This generator find them and can be used to relaunch those tasks automatically
    def incomplete_batch_generator():
        tasks = Task.incomplete_tasks_from_cache(data_dir)
        return tasks

    # select batchgen type
    def get_callback_batch_gen():
        batch_gen_type = input('What type of generator to run? Option: test, all, incomplete -> ')
        if batch_gen_type == 'test':
            return test_batch_generator
        elif batch_gen_type == 'all':
            return batch_generator
        elif batch_gen_type == 'incomplete':
            return incomplete_batch_generator
        else:
            print('Wrong input!')
            return get_callback_batch_gen()

    # callback to use
    callback_batch_gen = get_callback_batch_gen()

    # creating task cache
    Task.cache_tasks(callback_batch_gen(), data_dir)

    '''
    Extra command can be passed in launcher. For example following command is passed in the launcher to be added in sbatch.
    Please seperate all coammand with a new line(\n)
    Example: 
        sbatch_extra_cmd = "source activate rl\n"
        palab = PAlabLauncher(callback_batch_gen, sbatch_extra_cmd=sbatch_extra_cmd)
    '''
    if cluster == 'palab':
        palab = PAlabLauncher(callback_batch_gen)
        palab.launch()
    elif cluster == 'terra':
        # job_name is not required but can be given based on job submitted for easy tracking which jobs are running
        terra = TamuLauncher(callback_batch_gen, job_name='cmaes')
        terra.launch()
    else:
        raise ValueError('Invalid cluster name!!')


def main():
    parser = argparse.ArgumentParser()
    # there may be multiple different program to choose. Only one python example is given. Can also be run C++ executable and other programs also.
    parser.add_argument("--which", choices=['run_example_program'], required=True, help="Which program to run.")
    # which cluster to use
    parser.add_argument("--cluster", choices=['palab', 'terra'], required=True, help="Cluster name.")
    args = parser.parse_args()

    # which program to run
    if args.which == 'run_example_program':
        run_example_program(args.cluster)
    else:
        raise ValueError('Invalid program to execute!!')

if __name__ == "__main__":
    main()