import os
import time

# append readme text given a directory
def readme(dir, header=None):
    if not header:
        header = 'Give small description of readme'
    desc = input(f'{header} -> ')
    with open(os.path.join(dir, 'readme'), 'a') as f:
        f.write(f'Date created: {time.ctime()} -> {desc}\n\n')
