import os
import time
import json
from tqdm import tqdm


# append readme text given a directory
def readme(dir, header=None, desc=None):
    # if desc is given then no input is taken, rather write desc directly
    if not header:
        header = 'Give small description of readme'
    if desc is None:
        desc = input(f'{header} -> ')
    with open(os.path.join(dir, 'readme'), 'a') as f:
        f.write(f'Date created: {time.ctime()} -> {desc}\n\n')


# read json file. Each line contains single json output
def json_merge_read_single(path):
    res = []
    with open(path, 'r') as file:
        for line in file:
            v = json.loads(line)
            res.append(v)
    return res


# merge json file into single final output
def json_merge(dir_path, merged_file='all.final', recreate=True):

    merged_file_path = os.path.join(dir_path, merged_file)

    # if file exist and doesn't require recreation
    if os.path.isfile(merged_file_path) and not recreate:
        return merged_file_path

    # merge file
    with open(merged_file_path, 'w') as outfile:
        for filename in tqdm(os.listdir(dir_path), desc=f'Merging file in dir: {dir_path}'):
            if filename.endswith(".out"):
                with open(os.path.join(dir_path, filename), 'r') as infile:
                    for line in infile:
                        outfile.write(line)
    return merged_file_path


# this is a combination of first json_merge and json_merge_read_single
def json_get_merged(dir_path, merged_file='all.final', recreate=True):
    path = json_merge(dir_path=dir_path, merged_file=merged_file, recreate=recreate)
    data = json_merge_read_single(path=path)
    return data
