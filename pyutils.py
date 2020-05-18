import os
import shutil
import time
import json
import sys
import dill

def mkdir_p(dir, verbose = False, backup_existing=False):
    '''make a directory (dir) if it doesn't exist'''
    if not os.path.exists(dir):  # directory does not exist
        if verbose is True: errprint(f'Created new dir named: {dir}')
        os.mkdir(dir)
    else:  # dir exist
        # renaming existing directory to a new dir name if directory is not empty
        if len(os.listdir(dir)) > 0 and backup_existing:
            # find new path that doesn't exist
            for i in range(10000):
                new_dir_path = f'{dir}_{i}'
                if not os.path.exists(new_dir_path):
                    break
            # renaming directory
            if verbose:
                errprint(f'Moving dir {dir} -> {new_dir_path}')
            os.rename(src=dir, dst=new_dir_path)
            # now creating dir
            os.mkdir(dir)
    return dir

def dir_choice(dir, verbose = True):
    if os.path.exists(dir):

        # number of file in the directory
        tot_file = len(os.listdir(dir))

        # take input to execute
        inp = input(f'{dir} exist! Total file inside: {tot_file}\nNew: n, Delete: d, Empty directory: e, Continue: c, Abort a -> ')
        if inp == 'e':
            if verbose is True: print(f'-> Cleaning inside of the dir: {dir}')
            shutil.rmtree(dir) # delete all files and folder inside it
            mkdir_p(dir)
        if inp == 'd':
            if verbose is True: print(f'-> Deleting the dir: {dir}')
            shutil.rmtree(dir)
            exit(0)
        if inp == 'a':
            if verbose is True: print('-> Aborting...')
            exit(0)
        if inp == 'n':
            # take new directory name
            new_dir = input(f'Enter your desired directory name: ')
            new_dir = os.path.join(os.path.dirname(dir), new_dir)
            # if new dir name exist do same process again
            return dir_choice(new_dir, verbose)
    else:
        mkdir_p(dir, verbose)

    return dir

def write_pickle(path, data):
    with open(path, 'wb') as file:
        dill.dump(data, file)

def read_pickle(path):
    with open(path, 'rb') as file:
        data = dill.load(file)
    return data

def files_with_extension(dir, extension, fullpath=True):
    # given directory and extension get all files.
    files = []
    for file in os.listdir(dir):
        if file.endswith(extension):
            if fullpath:
                files.append(os.path.join(dir, file))
            else:
                files.append(file)
    return files

def rename_files_with_extension(dir, from_ext, to_ext):
    files = files_with_extension(dir, extension=from_ext, fullpath=True)
    for file in files:
        file_new_name = f'{os.path.splitext(file)[0]}.{to_ext}'
        os.rename(src=file, dst=file_new_name)

def merge_dict(d,d1):
    for k,v in d1.items():
        if (k in d):
            d[k].update(d1[k])
        else:
            d[k] = d1[k]
    return d

tag_job_finished_successfully = 'JobFinishedSuccessfully'
def print_job_finished():
    sys.stderr.write(f'{time.ctime()}\n{tag_job_finished_successfully}\n')

def print_log(log):
    # print final log dictionary with termination message
    js = json.dumps(dict(log))
    print(js)
    # tag use to identify successfully finished job
    print_job_finished()

def normalize(prob):
    tot = sum(prob)
    p = prob[:]
    if tot == 0: return p  # avoid dividing by 0
    for i in range(len(p)):
        p[i] /= tot
    return p

def errprint(str, flush=True, time_stamp=True, new_line=True):
    # By default flush, add \n at end and time
    if time_stamp: str = f'Time: {time.ctime()} > {str}'
    if new_line: str = f'{str}\n'
    sys.stderr.write(str)
    if flush: sys.stderr.flush()