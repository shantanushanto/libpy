import sys
import os

import numpy as np
import pandas as pd
from collections import defaultdict
import datetime


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

    def get_as_dataframe(self) -> pd.DataFrame:
        if self._row:
            print(f'Last row is not done. done_row?', file=sys.stderr)

        df = pd.DataFrame(self._rows)
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


def to_html(df):
    pd.set_option('colheader_justify', 'center')  # FOR TABLE <th>

    css_style = '''
    <style>
            /* includes alternating gray and white with on-hover color */
            .mystyle {
                font-size: 11pt; 
                font-family: Arial;
                border-collapse: collapse; 
                border: 1px solid silver;
            
            }
            
            .mystyle td, th {
                padding: 5px;
            }
            
            .mystyle tr:nth-child(even) {
                background: #E0E0E0;
            }
            
            .mystyle tr:hover {
                background: silver;
                cursor: pointer;
            }
    </style>
    '''

    html_string = '''
    <html>
      <head><title>HTML Pandas Dataframe with CSS</title></head>
        {css}
      <body>
        {table}
      </body>
    </html>.
    '''

    # OUTPUT AN HTML FILE
    html = html_string.format(css=css_style, table=df.to_html(classes='mystyle'))
    print(html, end='')
    return html


def col_arrange(col_orders, df, others_sorted=False) -> pd.DataFrame:
    """
    Arrange by col_orders and sort rest of them
    :param col_orders: optional (ignore): add prefix_ to filter out multiple columns with some prefix. e.g. prefix_z
    :param df:
    :param others_sorted: if sort other columns name
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
    others_col = list(set(list(df)) - set(new_col_order))
    if others_sorted:
        others_col = list(sorted(others_col))
    new_col_orders = new_col_order + others_col
    return df[new_col_orders]


def replicate_rows_between_date(sub: pd.DataFrame, date_col):
    """
    Replicate rows between two datetime
    :param sub:
    :param date_col: column name that holds date
    :return:
    """
    df_fill = pd.DataFrame()
    keep_cols = list(sub)

    symbols = sub['symbol'].unique()
    for symbol in symbols:
        df_tmp = sub[sub['symbol'] == symbol]
        df_tmp = df_tmp.sort_values(by=date_col).copy()

        df_tmp['_to'] = df_tmp[date_col].shift(-1)

        # filling gap
        for index, row in df_tmp.iterrows():

            # find date from -> to
            if pd.isnull(row['_to']):  # for last row create data for one forward quarter
                date_from, date_to = row[date_col], row[date_col] + datetime.timedelta(days=30 * 3)
            else:
                date_from, date_to = row[date_col], row['_to'] - datetime.timedelta(days=1)

            # replicate row with new date
            for date in pd.date_range(date_from, date_to):  # including extreme points
                new_row = row
                new_row[date_col] = date

                df_fill = df_fill.append(new_row)

    sub = df_fill[keep_cols]
    return sub


def pd_set_display(max_col=True, max_row=True, col_wrap=False, max_col_width=None):
    """

    :param max_col:
    :param max_row:
    :param col_wrap: wrap the column while printing
    :param max_col_width: set maximum column width. Use 100 for large string print
    :return:
    """
    if max_col:
        pd.set_option("max_columns", None)  # Showing only two columns
    if max_row:
        pd.set_option("max_rows", None)

    if not col_wrap:
        pd.set_option('display.expand_frame_repr', False)

    if max_col_width:
        pd.options.display.max_colwidth = max_col_width


def print_all(df, max_col_width=None, num_comma_sep=True, meta_info=True, rounding=True, freeze_x=10000, freeze_y=30, title=''):
    """
    Print all rows and columns
    :param df:
    :param max_col_width: maximum col width to print larger string
    :param freeze_x: put header after every freeze_x row
    :return:
    """
    pd_set_display(max_col_width=max_col_width)

    if num_comma_sep:
        pd.options.display.float_format = '{:,}'.format

    if rounding:
        df = df.round(2)

    for idx, j in enumerate(range(freeze_y, len(list(df)), freeze_y)):
        name = df.index.name if df.index.name is not None else 'index'
        df.insert(j+idx, name, df.index, allow_duplicates=True)  # +idx to increase the position 1 when inserting

    if title != '':
        header(title)

    for i in range(0, len(df), freeze_x):
        print(df.iloc[i: i + freeze_x])

    if meta_info:
        print(list(df))
        print(df.shape)


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


def common_cols(ldf, rdf):
    """
    Get common columns between two dataframe
    :param ldf:
    :param rdf:
    :return:
    """
    cols = intersect(list(ldf), list(rdf))
    return cols


def percentage(target, base, round=2, how='change'):
    """

    :param target:
    :param base:
    :param round:
    :param how: [change, normal] method to calculate
    :return:
    """
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
    elif sz == 2:
        eq = "".join(['-' for _ in range(len(line))])
        para = f'{line}\n{eq}'
    else:
        para = line

    print(para)

# -------------------------------------
#         Google sheet upload
# -------------------------------------


def upload_to_gsheet(df, sheet_id, sheet_name, verbose=True, stripe=True, fit_column=True):
    import gspread

    try:
        gc = gspread.oauth()
        sh = gc.open_by_key(sheet_id)
    except:
        path_home = os.path.expanduser('~')
        path_file = '.config/gspread/authorized_user.json'
        path = os.path.join(path_home, path_file)

        # if token expired delete the auth token and try again
        ans = input(f'token expired. want to delete and try again? {path} [y/n] ')
        if ans != 'y':
            exit(0)

        os.remove(path)
        upload_to_gsheet(df=df, sheet_id=sheet_id, sheet_name=sheet_name, verbose=verbose)
        return


    try:  # create new sheet if not exist
        ws = sh.worksheet(sheet_name)
        # sh.del_worksheet(ws)
    except:
        ws = sh.add_worksheet(title=f"{sheet_name}", rows="100", cols="26")

    # clearing the worksheet
    ws.clear()

    df = df.round(2)
    df = df.replace([np.inf, -np.inf], np.nan)
    df = df.fillna('')
    # values = df.round(2).fillna('').values.tolist()
    values = df.values.tolist()

    title = list(df)
    values = [title] + values

    ws.update('A1', values)

    ws.freeze(rows=1, cols=1)

    # formatting rules
    rules = {"requests": []}

    sheetID = ws.id

    X, Y = df.shape[0]+1, df.shape[1]

    # rule alternate color
    rule_ac = {
        "addConditionalFormatRule": {
            "rule": {
                "ranges": [
                    {
                        "sheetId": 0,
                        "startColumnIndex": 0,
                        "endColumnIndex": 2,
                        "startRowIndex": 1,
                        "endRowIndex": 10
                    }
                ],
                "booleanRule": {
                    "condition": {
                        "type": "CUSTOM_FORMULA",
                        "values": [
                            {
                                "userEnteredValue": "=ISEVEN(ROW())"
                            }
                        ]
                    },
                    "format": {
                        "backgroundColor": {
                            "red": 207 / 255,
                            "green": 211 / 255,
                            "blue": 253 / 255,
                        }
                    }
                }
            },
            "index": 0
        }
    }

    rule_ac['addConditionalFormatRule']['rule']['ranges'][0]['sheetId'] = sheetID
    rule_ac['addConditionalFormatRule']['rule']['ranges'][0]['endColumnIndex'] = Y
    rule_ac['addConditionalFormatRule']['rule']['ranges'][0]['endRowIndex'] = X

    # column auto fit rule
    rule_col_autofit = {
        "autoResizeDimensions": {
            "dimensions": {
                "sheetId": 0,
                "dimension": "COLUMNS",
                "startIndex": 0,
                "endIndex": 3
            }
        }
    }

    rule_col_autofit['autoResizeDimensions']['dimensions']['sheetId'] = sheetID
    rule_col_autofit['autoResizeDimensions']['dimensions']['endIndex'] = Y

    if stripe:
        rules['requests'].append(rule_ac)

    if fit_column:
        rules['requests'].append(rule_col_autofit)

    if 'requests' in rules:
        ws.spreadsheet.batch_update(rules)

    print(f'Uploaded to google sheet: {sheet_name} {sheet_id}', file=sys.stderr)

    return
# -------------------------------------
#         Math related formula
# -------------------------------------
def zscore(col):
    z = (col-np.mean(col)) / np.std(col)
    return z


def pct_growth(col):
    res = col.diff() / col.abs().shift()
    return res


def main():
    df = pd.DataFrame([[1], [2], [-1], [0], [-3]], columns=['val'])
    # df['val'] = pd.Series(np.array([1, 2, -1, 0, -3]))
    import router
    upload_to_gsheet(df, sheet_id=router.gsheet_id, sheet_name='test')


if __name__ == '__main__':
    main()
