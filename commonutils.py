import os
import time
import json
from tqdm import tqdm

from libpy import pyutils


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
def json_merge(dir_path, merged_file='all.final', recreate=True, merge_file_endswith='.out'):
    merged_file_path = os.path.join(dir_path, merged_file)

    # if file exist and doesn't require recreation
    if os.path.isfile(merged_file_path) and not recreate:
        return merged_file_path

    # merge file
    with open(merged_file_path, 'w') as outfile:
        for filename in tqdm(os.listdir(dir_path), desc=f'Merging file in dir: {dir_path}'):
            if filename.endswith(merge_file_endswith):
                with open(os.path.join(dir_path, filename), 'r') as infile:
                    for line in infile:
                        outfile.write(line)
    return merged_file_path


# this is a combination of first json_merge and json_merge_read_single
def json_merge_and_get_data(dir_path, merged_file='all.final', recreate=True, merge_file_endswith='.out'):
    path = json_merge(dir_path=dir_path, merged_file=merged_file, recreate=recreate,
                      merge_file_endswith=merge_file_endswith)
    data = json_merge_read_single(path=path)
    return data


# transfer file between clusters given full path in both side
def scp(letters: list, dir_copy: bool = False, check_exist: bool = True, verbose: bool = False,
        prod: bool = False) -> None:
    # given a list transfer file between cluster
    # letters: list of [from[0], to[1]] list to transfer file.
    # recursive: for dir transfer

    # check for files overwritten
    if check_exist:
        files_to_overwrite = []
        for letter in letters:
            to_path = letter[1]
            pyutils.Validation.overwriting(path=to_path, verbose=verbose)
            files_to_overwrite.append(to_path)

        pyutils.ActionRouter(f'Total {len(files_to_overwrite)}/{len(letters)} files to overwrite?', default_act_use=['abort', 'continue']).ask()

    if not prod:
        print('scp testing...')
    # transfer files
    for letter in tqdm(letters, desc='Transferring file (scp)'):
        from_path = letter[0]
        to_path = letter[1]

        extra_cmd = '-r' if dir_copy else ''

        cmd = f'sshpass -p $tamu_pss scp -q {extra_cmd} {from_path} {to_path}'
        if verbose:
            print(f'{cmd}')

        if prod:
            os.system(cmd)


# full path across clusters are different. Thus get path after the project root.
# Assuming Project root name is unique in the path
def path_from_project_root(path, project_name, add_project_dir=False, discard_last=True):
    # add_project_dir: add the project dir in front of the returned path

    dir_name = path.split('/')

    # assuming project name will be unique in the path
    no_name = dir_name.count(project_name)
    if no_name > 1:
        raise ValueError('Multiple dir with same project name')
    elif no_name == 0:
        raise ValueError('No project name in path')

    # match project name start index
    project_st_inx = dir_name.index(project_name)

    if add_project_dir:
        dir_name = dir_name[project_st_inx:]
    else:
        dir_name = dir_name[project_st_inx+1:]

    path_starting_project = os.path.join(*dir_name)

    if discard_last:
        path_starting_project = os.path.split(path_starting_project)[0]

    return path_starting_project


# get full local path as given remote path. Determined by project root and project prefix is appended
def path_local_as_remote(project_root, path, discard_last=False):
    project_name = os.path.basename(project_root)
    local_path = os.path.join(project_root,
                              path_from_project_root(path=path, project_name=project_name, discard_last=discard_last))
    return local_path


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--dir", required=True, help="dir to merge file")
    parser.add_argument("--merge_file_endswith", required=False, default='.out', type=str, help="Merge file endswith")
    args = parser.parse_args()

    json_merge(dir_path=args.dir, merge_file_endswith=args.merge_file_endswith)
