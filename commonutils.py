import os
import time

from libpy.JobLauncher import PAlabLauncher, AtlasLauncher, TamuLauncher, TerraGPULauncher

# append readme text given a directory
def readme(dir):
    desc = input('Give small description of the experiment. -> ')
    with open(os.path.join(dir, 'readme'), 'a') as f:
        f.write(f'Date created: {time.ctime()} -> {desc}\n\n')


# cluster launch job
def launch_job(cluster, callback_batch_gen, job_name, no_cpu=1, no_exlude_node=1, submission_check=False):
    sbatch_extra_cmd = "source activate rl\n"
    # choose cluster
    if cluster == 'palab':
        server = PAlabLauncher(callback_batch_gen, sbatch_extra_cmd=sbatch_extra_cmd, no_cpu_per_task=no_cpu, no_exclude_node=no_exlude_node, submission_check=submission_check)
    elif cluster == 'atlas':
        server = AtlasLauncher(callback_batch_gen, sbatch_extra_cmd=sbatch_extra_cmd, no_cpu_per_task=no_cpu, submission_check=submission_check)
    elif cluster == 'tamulauncher':
        server = TamuLauncher(callback_batch_gen, job_name=job_name, submission_check=submission_check)
    elif cluster == 'terragpu':
        import router  # as router may not be present in every project importing here
        sbatch_extra_cmd = f'source {os.path.join(router.project_root, "TerraModule.sh")}'
        server = TerraGPULauncher(callback_batch_gen, acc_id=122818929441, sbatch_extra_cmd=sbatch_extra_cmd, submission_check=submission_check)
    else:
        raise ValueError('Invalid cluster name!!')

    # launch jobs
    server.launch()
