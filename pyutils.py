import os
import shutil
import time
import json
import sys
import dill
from typing import List
from collections import defaultdict
import re
import datetime
import uuid
from functools import wraps

from libpy import commonutils


def mkdir_p(dir, verbose=False, backup_existing=False, if_contains=None):
    '''make a directory (dir) if it doesn't exist'''
    # todo: add recursive directory creation
    # todo: give a msg when dir exist but empty
    # dir: given full path only create last name as directory
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


def dir_choice(dir, verbose=True):
    if os.path.exists(dir) and len(os.listdir(dir)) > 0:

        # number of file in the directory
        tot_file = len(os.listdir(dir))

        # take input to execute
        inp = input(
            f'{dir} exist! Total file inside: {tot_file}\nNew: n, Delete: d, Empty directory: e, Continue: c, Abort: a, Backup: b -> ')
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
            if verbose:
                commonutils.readme(dir, header='Why want to backup?')
                print(' -> Backing up and creating new directory..')
            mkdir_p(dir, verbose=verbose, backup_existing=True)
        elif inp == 'c':
            pass
        else:
            if verbose: print(' -> Invalid option chosen..')
            dir_choice(dir=dir, verbose=verbose)
    else:
        mkdir_p(dir, verbose)

    return dir


def dir_backup(path_dir):
    """
    If directory exists make a copy and backup. Not deleting the keep_current_ticker one
    :param path_dir:
    :return:
    """

    def helper_copy_dir(src, dst):
        import errno
        try:
            shutil.copytree(src, dst)
        except OSError as exc:  # python >2.5
            if exc.errno == errno.ENOTDIR:
                shutil.copy(src, dst)
            else:
                raise

    if os.path.exists(path_dir) and len(os.listdir(path_dir)) > 0:
        path, dir_name = os.path.split(path_dir)

        # create unique directory name
        backup_dir_name = f'{dir_name}_{datetime.datetime.today().date()}_{uuid.uuid4()}'
        backup_path_dir = os.path.join(path, backup_dir_name)

        print(f'Exists: {path_dir} Backing up: {backup_path_dir}', file=sys.stderr)

        helper_copy_dir(src=path_dir, dst=backup_path_dir)


class ActionRouterClass:
    """
    ================
     Usage example:
    ================

    path = '/home/work/RPS/data'

    def callback_yes(*args, **kwargs):
        vpath = kwargs['path']
        print('Full path is: ', vpath)

    def callback_no(*args, **kwargs):
        vpath = kwargs['path']
        cnt = len(vpath.split('/'))
        print(f'Segment in path: ', cnt)

    (ActionRouter('Do you want to copy?', default_act_use=['abort'])
     .add('yes', callback_yes, 10, path=path)
     .add('no', callback_no, path=path)
     .ask())
    """
    # Copy files? [continue (c), abort (a), use_cluster (u), undo (un)] -> {input}
    def __init__(self, header: str, default_act_use=None):
        if default_act_use is None:
            default_act_use = []

        self.header = header

        self._default_act_use = default_act_use  # default action to add in actions list

        # map input option to function
        self.actions = {}  # contain {'func': callback, 'args': (), 'kwargs': {}}

        # mapping short option to long option
        self._short_opt = {}  # short -> opt mapping
        self._opt_short = {}  # opt -> short mapping

        self._last_opt_choosen = []  # keep all track of the option choosen last

        self.ask_ret = None  # last executed ask callback return value

        # initializing
        self._init()

    # generate all default actions in a dictionary. It will be updated time to time
    def _default_actions(self) -> dict:

        def callback_abort(*args, **kwargs):
            sys.exit(0)

        def callback_continue(*args, **kwargs):
            pass

        actions = {'abort': {'func': callback_abort, 'args': (), 'kwargs': {}},
                   'continue': {'func': callback_continue, 'args': (), 'kwargs': {}}}
        return actions

    # initialize first
    def _init(self):
        # adding default actions
        self._add_default_actions(default_act_use=self._default_act_use, actions=self.actions)

    # add default actions to the original actions
    def _add_default_actions(self, default_act_use: List[str], actions: dict):
        # default_act_use: default actions to use
        # actions: default actions where to add

        default_actions = self._default_actions()
        for action in default_act_use:
            try:
                act = default_actions[action]
                actions[action] = act
            except KeyError:
                raise KeyError

    # generate short option from option and map
    # it is called when ask is called. Short option are generated from sorted long option
    def _map_short_opt(self):

        # reset previous mapping key
        self._short_opt = {}
        self._opt_short = {}

        # get all option keys
        opts = self.actions.keys()
        for opt in sorted(opts):

            # find unique short prefix
            for i in range(len(opt)):
                sopt = opt[:i+1]
                if sopt not in self._short_opt:
                    # map short opt to opt
                    self._short_opt[sopt] = opt
                    self._opt_short[opt] = sopt
                    break

    # generate option header with short option
    def _gen_opt_header(self):
        # mapping short option to long option
        self._map_short_opt()

        # generating header
        opt_str = ", ".join(f'{k} ({self._opt_short[k]})' for k, v in self.actions.items())

        opt_str = f'[{opt_str}]'
        return opt_str

    # get last nth option. Default last. Return the full option name
    def last_opt(self, n=0):
        try:
            return self._last_opt_choosen[-n]
        except KeyError:
            raise KeyError

    # add action
    def add(self, opt, func, *args, **kwargs):
        if opt in self.actions:
            raise ValueError(f'{opt} is already taken for action option.')
        # register option to action
        self.actions[opt] = {'func': func, 'args': args, 'kwargs': kwargs}

        return self

    # return last callback return value
    def ret(self):
        return self.ask_ret

    # ask for input
    def ask(self):
        opt_header = self._gen_opt_header()
        inp = input(f'{self.header} {opt_header} -> ')

        try:
            # get opt from either first check short and then long opt
            if inp in self._short_opt:
                opt = self._short_opt[inp]
            elif inp in self.actions:
                opt = inp
            else:
                raise

            # storing last option
            if len(self._last_opt_choosen) == 0 or opt != self.last_opt():
                self._last_opt_choosen.append(opt)

            # get function and their arguments
            cb = self.actions[opt]
            # call callback function
            self.ask_ret = cb['func'](*cb['args'], **cb['kwargs'])

            # if recall is returned in a dictionary by the function then it's a loop to ask the same question again
            if type(self.ask_ret) == dict and 'type' in self.ask_ret and self.ask_ret['type'] == 'recall':
                self.ask()

        except KeyError:
            raise ValueError('Invalid option.')
        return self


# create a action router class and return for chaining access
def ActionRouter(header='', default_act_use=None):
    ar = ActionRouterClass(header=header, default_act_use=default_act_use)
    return ar

# ---------- End of ActionRouter ----------


class Validation:
    def __init__(self):
        pass

    @staticmethod
    # checking if overwriting anything that is existing previously
    def overwriting(path, verbose=True):

        # path is a file and exist
        if os.path.isfile(path):
            if verbose: errprint(f'{path} Exists!', time_stamp=False)
            return True

        # path is directory and not empty
        if os.path.isdir(path) and len(os.listdir(path)) > 0:
            no_file = len(os.listdir(path))
            if verbose: errprint(f'{path} Exists! File inside {no_file}', time_stamp=False)
            return True

        return False


def write_pickle(path, data):
    with open(path, 'wb') as file:
        dill.dump(data, file)


def read_pickle(path):
    with open(path, 'rb') as file:
        data = dill.load(file)
    return data


def write_text(path, data):
    with open(path, 'w') as file:
        file.write(data)


def read_text(path, as_multi_line=False):
    '''
    read text from a text file
    :param path:
    :param as_multi_line: get the text as lines in an array
    :return:
    '''
    with open(path, 'r') as file:
        if as_multi_line:
            data = file.readlines()
        else:
            data = file.read()
    return data


def get_files_with_extension(dir, extension, fullpath=True):
    # new function with name udpate for a more intuitive method name
    return files_with_extension(dir=dir, extension=extension, fullpath=fullpath)


def files_with_extension(dir, extension, fullpath=True):
    '''
    given directory and extension get all files.
    :param extension: extension is check with endswith. adding dot is optional.
    '''
    files = []
    for file in os.listdir(dir):
        if file.endswith(extension):
            if fullpath:
                files.append(os.path.join(dir, file))
            else:
                files.append(file)
    return files


def get_date_from_path(path):
    """
    Extract date from path/any string. It'll handle full path automatically
    :param line:
    :return: if date is not found return the full string as it is
    """
    # find base file name if full path is given
    line = os.path.basename(path) if os.path.isabs(path) else path

    # extract date
    try:
        d = re.search(r'\d{4}-\d{2}-\d{2}', line)[0]
        return d
    except:
        return line


# get recent file by date. File has name pattern of prefix_date.extension
def get_last_dated_file(dir, prefix, extension='.csv', fullpath=True, N=1, date_type='YMD', date_anywhere=True):
    """
    Get recent file by date. File has name pattern of prefix_date.extension
    :param dir:
    :param prefix:
    :param extension:
    :param fullpath:
    :param N: if N > 1 return recent N dated files
    :param date_type: [YMD, timestamp] current implementation only work if timestamp is at last
    :param date_anywhere: find date from file name dynamically
    :return:
    """

    # current implementation only work if timestamp is at last
    if date_type == 'timestamp':
        date_anywhere = False

    # get all file with extension
    files = files_with_extension(dir=dir, extension=extension, fullpath=False)
    # filter out by matching prefix, assuming date is in last
    files = [file for file in files if file.startswith(prefix)]

    if date_anywhere:
        # dynamically find date in file and sort accordingly
        files = sorted(files, key=lambda x: get_date_from_path(x))
    else:
        # assume date is at end
        files = sorted(files, key=lambda x: x.split(prefix)[1])

    # return last dated file if exist otherwise return None
    try:
        last_files = files[-N:]

        # generate full path
        if fullpath:
            last_files = [os.path.join(dir, last_file) for last_file in last_files]
        # return only last file or N recent files in list
        return last_files[-1] if N == 1 else last_files
    except:
        return None


def rename_files_with_extension(dir, from_ext, to_ext, verbose=True):
    # from_ext: Pass extension name with . if necessary.
    # to_ext: Only extension name. No dot included. (.) is added so no need to pass .
    files = files_with_extension(dir, extension=from_ext, fullpath=True)
    for file in files:
        file_new_name = f'{os.path.splitext(file)[0]}.{to_ext}'
        os.rename(src=file, dst=file_new_name)
        if verbose:
            errprint(f'Renaming: {file} -> {file_new_name}')
    if verbose:
        errprint(f'Total files renamed: {len(files)}')


def merge_dict(d, d1):
    for k, v in d1.items():
        if (k in d):
            d[k].update(d1[k])
        else:
            d[k] = d1[k]
    return d


# convert argument to dictionary
def arg_to_dict(args):
    log = dict()
    # putting all arg in log
    for arg in vars(args):
        log[arg] = getattr(args, arg)

    return log


tag_job_finished_successfully = 'JobFinishedSuccessfully'


def print_job_finished():
    sys.stderr.write(f'{time.ctime()}\n{tag_job_finished_successfully}\n')


# get incomplete task by finish tag
def is_job_finished(file_path, finish_tag=tag_job_finished_successfully, by='any'):
    # file_path: raw full file path without any extension
    # finish_tag: default finish tag
    # by = any: True of False any of the criteria fulfilled. cat: return code by category not found
    err_path = f'{file_path}.err'
    out_path = f'{file_path}.out'

    cat_code = 'unknown'  # successfull, file_not_exist, error_in_file, err_file_empty, out_not_exist,

    # check for finish tag in err
    if os.path.isfile(err_path):  # check if file exist but finish tag doesn't exist
        with open(err_path, 'r') as f:
            # check finish tag
            file_str = f.read()
            if finish_tag in file_str:
                # out file exists and size is > 0
                if os.path.isfile(out_path):
                    if os.stat(out_path).st_size > 0:
                        cat_code = 'successfull'
                    else:
                        cat_code = 'err_file_empty'
                else:
                    cat_code = 'out_not_exist'
            else:
                if 'error' in file_str.lower():
                    cat_code = 'error_in_file'
                else:
                    cat_code = 'no_finish_tag'
    else:
        cat_code = 'file_not_exist'

    found = True if cat_code == 'successfull' else False
    if by == 'any':
        return found
    elif by == 'cat':
        return {
            'found': found,
            'cat': cat_code
        }
    else:
        raise ValueError('Invalid by option')


def measure(func, as_='s'):
    name_func = func.__name__
    @wraps(func)
    def _time_it(*args, **kwargs):
        start = int(round(time.time() * 1000))
        try:
            return func(*args, **kwargs)
        finally:
            end_ = int(round(time.time() * 1000)) - start
            msec = end_ if end_ > 0 else 0
            if as_ == 's':
                tot = msec / 1000
            elif as_ == 'm':
                tot = msec / (1000 * 60)
            elif as_ == 'ms':
                tot = msec

            print(f"Execution time ({name_func}): {tot:.2f} {as_}")
    return _time_it


def print_log(log):
    # print final log dictionary with termination message
    js = json.dumps(dict(log))
    print(js)
    # tag use to identify successfully finished job
    print_job_finished()


def normalize(prob, make_pos=False):
    p = prob[:]
    if make_pos:
        mn = min(p)
        if mn < 0:
            # make all value positive
            mn = abs(mn) # make the negative value positive
            for i in range(len(p)):
                p[i] += mn

    tot = sum(p)

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


# convert list of dict to dict of list
def dict_list(lofd):
    # lofd: list of dict, dofl: dict of list
    # [{id: 1, name: a}, {id: 2, name: b}, {id:3, name: c}] -> {id: [1, 2, 3], name: [a, b, c]}
    dofl = defaultdict(list)
    for d in lofd:
        for k, v in d.items():
            dofl[k].append(v)
    return dofl


def set_seed(seed):
    import random
    import numpy as np

    random.seed(seed)
    np.random.seed(seed + 1)
    try:
        import tensorflow as tf
        tf.random.set_seed(seed + 2)
    except:
        pass


if __name__ == '__main__':
    mkdir_p('.tmp', verbose=True, backup_existing=True, if_contains='.py')