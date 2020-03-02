# How to use tamulauncher with other job submission manager
### Introduction
I had a project that has tons of small programs to run for around 30 minutes each with different
arguments. At first I used TAMU Terra cluster and submitted all the jobs one by one with sbatch.
There were around 50,000 jobs. At some point I came to know about Tamulauncher and suggested to use
it instead of submitting single jobs. Also in our lab we have our own small cluster with around 100 cores.
I wanted to use all of the resources I had and created this module. I thought this might
help others too.

I have two modules here.
* PalabLauncher: It is used to submit jobs in the slurm. Can also be used in Terra cluster or any slurm based 
job management cluster.
* Tamulauncher: TAMU's special launcher to run lots of small program at once.

### How to use
* Run a python program: This example will run example_program.py with different arguments with slurm job submission. 
Use <code>tamulauncher</code> in --cluster argument to use Tamulauncher<br>
<code> python example_batch.py --which example_program_py --cluster palab </code>
* Run a C++ program: This example will run example_program.cpp with different arguments. <br>
<code> python example_batch.py --which example_program_cpp --cluster palab </code>

### Creating Task
Job launcher takes a callback function that produce all task to submit. See Task class in JobLauncher. It contains
two string cmd and output file name.
* cmd: Store program run command with all arguments. See example_batch.py for example.
* out: Output file name for that program. Give a file name without any extension. For each program it will create
two output. One with <b>.out</b> and another with <b>.err</b>. Usually <b>.out</b> contains program result output and
<b>.err</b> contains debug/log output.

<b>See example_batch.py to how to create the callback function to pass to the JobLauncher.</b>

### Batch generator
For each of the program run I used 3 type of batch generators.
* test: As there are lots of jobs to run it is wise to run some small test programs to check if everything is good.
Use test to run small number of programs with different parameter. 
* all: To run full scale experiment.
* incomplete: Sometime it might happen that some jobs are failed or you may need to cancel the jobs and submit later. While
submitting the jobs it creates cache of the submitted jobs and can find out which program are successfully finished and which are not.
Based on that it can rerun previously incomplete programs again. It can use recently submitted jobs and previously submitted jobs also.
See JobLauncher/Task.incomplete_tasks_from_cache method for more details.

### Thanks
* Any question? email me github username @ gmail.com.
* Found any bug? raise an issue.
* Like it? give a star in this repo.