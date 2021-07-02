from collections import OrderedDict, defaultdict
import itertools
import os
from typing import List

import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

from libpy import JobLauncher, pyutils, commonutils
import router


class ExpParam:

    def __init__(self):
        self.param = OrderedDict()

    def _get_args_order(self):
        # return args order based on depend on self.param
        # TODO: handle depend check, handle file name order and execution order

        keys = list(self.param.keys())
        args_order = []

        t = 0
        while len(keys) > 0:
            t += 1
            if t > 10000:
                raise ValueError('Param depend check is in loop')

            # check all key depend already in args_order. So that it can be taken
            def can_take(key):
                cons = self.param[key].get('depend', [])
                for c in cons:
                    if c not in args_order:
                        return False
                return True

            key = keys[0]
            # if can take, take it and remove from the keys
            if can_take(key):
                args_order.append(key)
                keys = keys[1:]
            else:
                keys[0], keys[1] = keys[1], keys[0]

        return args_order

    def describe(self, verbose=True, key=None):
        # key: provide key to get description for single value

        descs = []

        args_order = self._get_args_order()
        for idx, k in enumerate(args_order):
            v = self.param[k]
            desc = None
            if 'desc' in v:
                desc = v['desc']
            elif 'depend' in v:
                desc = f'(depend on) {v["depend"]}'
            else:

                val = v["func"]()
                if type(val) == list and len(val) > 30:
                    desc = f'(#items: {len(val)})'
                else:
                    desc = f'{val}'

            if key:
                if k == key:
                    if verbose:
                        pyutils.errprint(f'{k} : {desc}', time_stamp=False)
                    return (k, desc)
            else:
                descs.append((k, desc))

        if verbose:
            mx_offset = max([len(k) for k, _ in descs])
            pyutils.errprint(f'Description (#param: {len(descs)}):', time_stamp=False)
            for idx, (k, desc) in enumerate(descs):
                pyutils.errprint(f'{idx + 1:02d}. {k:<{mx_offset}} : {desc}', time_stamp=False)

    # add argument
    def add(self, name, verbose=True, **kwargs):
        """
        args contain:   func(function): generate parameter value,
                        depend (list[str]): parameter list need to be generated before this one (args_order)

        func definition and example:
        ----------------
            def func(**kwargs):
                if 'prg_len' not in kwargs:
                    raise ValueError('"prg_len" not in kwargs')
                else:
                    prg_len = kwargs['prg_len']

                prg_id = []
                for v in range(100):
                    if prg_len == 5:
                        prg_id.append(v+1)
                    else:
                        prg_id.append(v+2)

                return prg_id
        """

        # storing previous verbose if exist
        prev_desc = self.describe(key=name, verbose=False) if name in self.param else None

        self.param[name] = kwargs

        # for updating giving
        if prev_desc and verbose:
            cur_desc = self.describe(key=name, verbose=False)
            pyutils.errprint(f'Update: {name}: {prev_desc[1]} -> {cur_desc[1]}', time_stamp=False)

    # generate tasks arguments and return them in a list
    def generate(self) -> List:

        args_order = self._get_args_order()

        # contain tasks kwargs
        tasks_kwargs = []

        # single_args contains param for one run
        def gen_kwargs(single_args: OrderedDict, arg_names: list):
            if len(arg_names) == 0:
                # now construct Tasks from single_args
                tasks_kwargs.append(single_args)
                return

            # arg
            arg_name = arg_names[0]
            arg = self.param[arg_name]

            f = arg['func']
            f_val = f(**single_args)  # single_args may contain some value based to filter out something
            f_val = [f_val] if type(f_val) != list else f_val  # convert to list if single value

            for fv in f_val:
                # check if exist from previous
                # if arg_name in single_args:
                #   raise ValueError(f'{arg_name} should not be in single_args')

                fd = {arg_name: fv}
                gen_kwargs(single_args=OrderedDict({**single_args, **fd}), arg_names=arg_names[1:])

        gen_kwargs(single_args=OrderedDict(), arg_names=args_order)
        return tasks_kwargs


class Experiment:

    def __init__(self, exe, data_dir, exp_param, name=None):
        self.name = name if name else data_dir

        self.data_dir = os.path.join(router.expdata_root, data_dir)  # full path
        self.exe = os.path.join(router.project_root, exe)  # full path

        self.exp_param = exp_param

    def _create_task(self, **kwargs):
        # Job name is used as file name also. Make it unique
        job_name, cargs = JobLauncher.gen_job_name(kwargs=kwargs, data_dir=self.data_dir)

        # Joblauncher create .out and .err both with out name
        out = os.path.join(self.data_dir, job_name)

        cmd = f'python {self.exe} {cargs}'
        return JobLauncher.Task(cmd, out)

    # get final.all sync from remote to local
    def sync(self, cluster='atlas'):
        import sync
        sync.transfer_all(dir_name=os.path.basename(self.data_dir), cluster=cluster)

        all_final = os.path.join(self.data_dir, 'all.final')
        num_lines = len(list(open(all_final)))
        num_tasks = len(self.get_tasks_gen()())
        pyutils.errprint(f'Final result: Total tasks {num_lines}/{num_tasks}', time_stamp=False)

    # return generator to create task
    def get_tasks_gen(self):
        def gen():
            tasks = []
            tasks_kwargs = self.exp_param.generate()
            for task_kwargs in tasks_kwargs:
                task = self._create_task(**task_kwargs)
                tasks.append(task)
            return tasks

        return gen

    # printing sample top n tasks
    def sample_task(self, n=5):
        task_gen = self.get_tasks_gen()
        tasks = task_gen()

        for i in range(min(len(tasks), n)):
            pyutils.errprint(f'{tasks[i]}', time_stamp=False)

    def run(self, cluster, prod):

        self.data_dir = pyutils.dir_choice(self.data_dir)
        # launch job in cluster
        JobLauncher.launch_job(
            cluster=cluster, callback_batch_gen=self.get_tasks_gen(), submission_check=not prod,
            job_name='cmaes_bin', acc_id=122818927574, time='15:00:00')

        # create readme file in the folder to easy remember
        commonutils.readme(self.data_dir)


class Query:

    def __init__(self, dir_path, name=''):
        self.name = name
        self.dir_path = dir_path  # where all.final data belongs
        self.df = None

    def get_df(self, header=None):
        if header:
            self.header(name=header)
        return self.df.copy()

    # get title with exp name and fig path
    def title(self, title):
        pre = '' if self.name == '' else f'{self.name} -'
        new_t = f'{pre} {title}'

        import re
        path_plain = re.sub('[^\w\-_\. ]', '_', new_t)
        fig_path = os.path.join(router.fig_root, path_plain.replace(' ', '_'))

        return new_t, fig_path

    # add a header
    def header(self, name):
        print(f'---------------\n  {name}\n---------------')

    # plot the figure
    def plot(self, title, ax, df_csv=None):
        new_t, fig_path = self.title(title=title)
        plt.title(new_t)
        plt.tight_layout()  # make room for x axis
        plt.grid()
        plt.savefig(fig_path)
        # plt.show()
        plt.draw()
        # ax.draw()

        if df_csv is not None:
            csv_path = f'{fig_path}.csv'
            df_csv.to_csv(csv_path)

    # plot a single column first sort then plot
    def single_col_plot(self, col_name):
        df = self.df.copy()
        df = df.sort_values(by=col_name)
        df['serial'] = np.arange(len(df))

        ax = df.plot.scatter(x='serial', y=col_name)
        self.plot(title=f'Single coloumn - [{col_name}]', ax=ax)

    # read the final data and construct dataframe
    def read(self, final='all.final'):
        df = Query.read_as_df(dir_path=self.dir_path, final=final)
        self.df = df
        return self

    def describe(self, check_avoid=True):
        Query.describe_df(df=self.df, check_avoid=check_avoid)

    @staticmethod
    # make a summary of passing df
    def describe_df(df, check_avoid=True, details_depth=10):
        # details_depth: max column unique value to describe

        # avoiding describe if lots of rows
        if check_avoid:
            if len(df) > 10000:
                pyutils.errprint(f"Avoiding dataframe describe. No of rows: {len(df)}", time_stamp=False)
                return

        cols = list(df)

        descs = []  # (no_name, name, desc)

        for col in cols:
            col_vals = df[col].value_counts(dropna=False)
            name = col
            no_name = len(col_vals)

            # unique column value more than 10
            if len(col_vals) < details_depth:
                row = ", ".join([f'{k} ({v})' for k, v in col_vals.iteritems()])
                desc = f'[{row}]'

                descs.append((no_name, name, desc))

        pyutils.errprint(f'Data summary: [Features: {len(list(df))}, Rows: {len(df)}]', time_stamp=False)
        pyutils.errprint(sorted(list(df), key=lambda x: x.lower()), time_stamp=False)

        for idx, (no_name, name, desc) in enumerate(sorted(descs)):
            pyutils.errprint(f'{idx + 1:02d}.{no_name}. {name}: {desc}', time_stamp=False)


    # run all registered result
    def result(self):
        raise NotImplementedError

    @staticmethod
    # read final data and convert to dataframe
    def read_as_df(dir_path, final='all.final'):
        final_path = os.path.join(dir_path, final)
        data = commonutils.json_merge_read_single(final_path)
        # read .final file and convert it to a df. Assuming all.final already upto date
        df = pd.DataFrame(data)
        return df

    @staticmethod
    # read multiple final data from different directory and merge them together with dir metadata in dataframe
    def get_data_from_dirs(folders, folders_name=None):
        # folders: folder name where exp .final data is
        # folders_name: use this instead of folder original name

        # if folders_name is not given use folders original name instead
        if folders_name is None:
            folders_name = folders[:]

        assert folders_name and len(folders) == len(folders_name)

        # getting all data from each of the directory
        df = pd.DataFrame()
        for folder, folder_name in zip(folders, folders_name):
            dir_path = os.path.join(router.expdata_root, folder)
            sub_df = Query.read_as_df(dir_path=dir_path)
            sub_df['folder'] = folder_name
            df = pd.concat([df, sub_df])

        return df
