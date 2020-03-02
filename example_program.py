import argparse
import time
import sys

import pyutils

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--number", required=True, help="Sample number", type=int)
    parser.add_argument("--sleep_time", required=True, help="Sample number", type=int)
    args = parser.parse_args()
    time.sleep(args.sleep_time)
    print(f'Current time: {time.ctime()}')
    print(f'Sample number is : {args.number}')
    print(f'Slept for: {args.sleep_time}')

    # this tag is necessary to check if it is finished successfully. Output will be written in .out file. And all debug message will be written in .err file. Thus, successful tag is written in stderr file.
    sys.stderr.write(f'{pyutils.tag_job_finished_successfully}\n')

if __name__=='__main__':
    main()
