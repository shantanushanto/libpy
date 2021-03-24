import sys

import numpy as np
import pandas as pd
from collections import defaultdict


class DictPanda:

    def __init__(self, verbose=True):
        self._verbose = verbose

        self._row = {}
        self._rows = []

    def add(self, key, value):
        """
        Add by key value pair
        :param key:
        :param value:
        :return:
        """
        if key in self._row.keys():
            if self._verbose:
                print(f'Key ({key}) already exists')

        self._row[key] = value
        return self

    def add_dict(self, d: dict):
        """
        add from a dictionary
        :return:
        """
        for key, value in d.items():
            self.add(key=key, value=value)
        return self

    def done_row(self):
        self._rows.append(self._row)
        self._row = {}
        return self

    def load_with_dataframe(self, df: pd.DataFrame):
        for ind, row in df.iterrows():
            self._rows.append(row.to_dict())
        return self

    def get_as_dataframe(self):
        df = pd.DataFrame()
        row: dict
        for row in self._rows:
            df_row = pd.DataFrame(data=[row.values()], columns=row.keys())
            df = pd.concat([df, df_row], sort=False)

        return df


# remove duplicated but keep the order
def remove_duplicates(seq):
    """
    Remove duplicated but keep the order
    :param seq:
    :return:
    """
    seen = set()
    seen_add = seen.add
    return [x for x in seq if not (x in seen or seen_add(x))]


def col_arrange(col_orders, df) -> pd.DataFrame:
    """
    Arrange by col_orders and sort rest of them
    :param col_orders: add prefix_ to filter out multiple columns with some prefix. e.g. prefix_z
    :param df:
    :return:
    """

    # return column name from df with prefix in sorted order
    def get_col_name_with_prefix(prefix):
        cols = [col for col in list(df) if col.startswith(prefix)]
        cols = sorted(cols)
        return cols

    # find col order
    new_col_order = []
    col: str
    for col in col_orders:
        # find out all column name if prefix_ tag present
        if col.startswith('prefix_'):
            col_prefix = col.split('prefix_')[1]  # find out which column to be taken
            new_col_order += get_col_name_with_prefix(prefix=col_prefix)
        else:  # put single column name
            if col in list(df):
                new_col_order += [col]

    new_col_order = remove_duplicates(new_col_order)
    new_col_orders = new_col_order + sorted(list(set(list(df)) - set(new_col_order)))
    return df[new_col_orders]


def pd_set_display(max_col=True, max_row=True):
    if max_col:
        pd.set_option("max_columns", None)  # Showing only two columns
    if max_row:
        pd.set_option("max_rows", None)


def print_all(df):
    """
    Print all rows and columns
    :param df:
    :return:
    """
    pd_set_display()
    print(df)


def intersect(a, b):
    # find intersection between two list
    in_both = list(set(a).intersection(set(b)))
    return in_both


def subset(sub, super, msg="check"):

    if set(sub).issubset(set(super)):
        return True
    else:
        if msg:
            print(f'sub({sub} is not a subset of super({len(super)}) {msg}', file=sys.stderr)

        return False


def percentage(target, base, round=2, how='change'):

    if how == 'change':
        val = ((target-base)/base) * 100
    elif how == 'normal':
        val = (target/base) * 100
    else:
        raise ValueError('Invalid option')

    if round:
        val = np.round(val, round)

    return val


def header(line, sz=1):
    if sz == 1:
        eq = "".join(['=' for _ in range(len(line))])
        para = f'{eq}\n{line}\n{eq}'
    else:
        para = line

    print(para)