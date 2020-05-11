import os
import time

# append readme text given a directory
def readme(dir):
    desc = input('Give small description of the experiment. -> ')
    with open(os.path.join(dir, 'readme'), 'a') as f:
        f.write(f'Date created: {time.ctime()} -> {desc}\n\n')
