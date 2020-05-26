import os
import shutil
import time
import json
import sys
import dill
import re

from libpy import commonutils

def mkdir_p(dir, verbose = False, backup_existing=False, if_contains=None):
    '''make a directory (dir) if it doesn't exist'''
    # if_contains: backup_existing True if_contains is a list. If there is any file/directory in the
    #              first (not recursive) pass match with if_contains then backup
    if not os.path.exists(dir):  # directory does not exist
        if verbose is True: errprint(f'Created new dir named: {dir}')
        os.mkdir(dir)
    else:  # dir exist
        need_backup = False
        # check if backup is needed
        if backup_existing and len(os.listdir(dir)) > 0:
            # check contains helper
            def helper_contains():
                # check if if_contains is a list
                if not isinstance(if_contains, list):
                    raise ValueError('if_contains in mkdir_p is not a list')

                files_inside = os.listdir(dir)
                for pat in if_contains:
                    if any(pat in file for file in files_inside):
                        return True
                return False

            if (if_contains is None) or (if_contains and helper_contains()):
                need_backup = True

        if need_backup:
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
        inp = input(f'{dir} exist! Total file inside: {tot_file}\nNew: n, Delete: d, Empty directory: e, Continue: c, Abort: a, Backup: b -> ')
        if inp == 'e':
            if verbose: print(f'-> Cleaning inside of the dir: {dir}')
            shutil.rmtree(dir)  # delete all files and folder inside it
            mkdir_p(dir)
        elif inp == 'd':
            if verbose: print(f'-> Deleting the dir: {dir}')
            shutil.rmtree(dir)
            exit(0)
        elif inp == 'a':
            if verbose: print('-> Aborting...')
            exit(0)
        elif inp == 'n':
            # take new directory name
            new_dir = input(f'Enter your desired directory name: ')
            new_dir = os.path.join(os.path.dirname(dir), new_dir)
            # if new dir name exist do same process again
            return dir_choice(new_dir, verbose)
        elif inp == 'b':
            if verbose: print(' -> Backing up and creating new directory..')
            commonutils.readme(dir, header='Why want to backup?')
            mkdir_p(dir, verbose=verbose, backup_existing=True)
        else:
            if verbose: print(' -> Invalid option chosen..')
            dir_choice(dir=dir, verbose=verbose)
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

def rename_files_with_extension(dir, from_ext, to_ext, verbose=True):
    files = files_with_extension(dir, extension=from_ext, fullpath=True)
    for file in files:
        file_new_name = f'{os.path.splitext(file)[0]}.{to_ext}'
        os.rename(src=file, dst=file_new_name)
        if verbose:
            errprint(f'Renaming: {file} -> {file_new_name}')
    if verbose:
        errprint(f'Total files renamed: {len(files)}')


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

if __name__ == '__main__':
    mkdir_p('.tmp', verbose=True, backup_existing=True, if_contains='.py')