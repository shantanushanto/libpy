import argparse
import time

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--number", required=True, help="Sample number")
    parser.add_argument("--sleep_time", required=True, help="Sample number")
    args = parser.parse_args()
    time.sleep(args.sleep_time)
    print(f'Current time: {time.ctime()}')
    print(f'Sample number is : {args.number}')
    print(f'Slept for: {args.sleep_time}')

if __name__=='__main__':
    main()