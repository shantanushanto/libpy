#include <iostream>
#include <time.h>
#include <unistd.h>
using namespace std;


int main(int argc, char**args) {
    int number = atoi(args[1]);
    int sleep_time = atoi(args[2]);

    usleep(sleep_time * 1000000); // in microseconds

    cout << "Current time: " << time(NULL) << endl;
    cout << "Sample number is : "<< number << endl;
    cout << "Slept for: "<< sleep_time << endl;

    // this tag is necessary to check if it is finished successfully. Output will be written in .out file. And all debug message will be written in .err file. Thus, successful tag is written in stderr file.
    string success_tag = "JobFinishedSuccessfully";
    cerr << endl << success_tag << endl;
    return 0;
}
