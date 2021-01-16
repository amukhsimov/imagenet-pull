import sys
import os
import time
import getopt
import multiprocessing as mp
from multiprocessing import Pool, Lock, Manager
import workers
import re
import class_distributer as cd
from constants import *

env = {}


def _set_args():
    env['n_jobs'] = 1
    env['threads-per-job'] = 10
    env['classes'] = None
    env['recursive'] = False
    env['max-classes'] = None
    env['pictures-per-class'] = 500
    env['dir'] = ''
    env['mode'] = MODE_LOAD_PICTURES
    env['release'] = 'fall2011'

    if len(sys.argv) > 1:
        try:
            args = getopt.getopt(sys.argv[1:], 'hj:t:c:RC:p:d:', [
                'help',
                'usage',
                'jobs=',
                'threads',
                'classes',
                'recursive',
                'max-classes',
                'pictures-per-class',
                'dir',
                'save-classes',
                'save-classes-only',
                'release',
            ])
            print(args[0])

            for key, val in args[0]:
                key = str(key).lstrip('-')

                if key in ('usage', 'help', 'h'):
                    print_usage(0)

                if key in ('jobs', 'j'):
                    env['n_jobs'] = int(val)

                elif key in ('threads', 't'):
                    env['threads-per-job'] = int(val)

                elif key in ('classes', 'c'):
                    if re.fullmatch(r'(\s*n\d{8}\s*,?)+', val):
                        env['classes'] = re.findall(r'n\d{8}', val)
                    else:
                        print_usage()

                elif key in ('pictures-per-class', 'p'):
                    env['pictures-per-class'] = int(val)

                elif key in ('dir', 'd'):
                    env['dir'] = val

                elif key in ('recursive', 'R'):
                    env['recursive'] = True

                elif key in ('max-classes', 'C'):
                    env['max-classes'] = int(val)

                elif key in ('save-classes',):
                    env['mode'] = MODE_LOAD_PICTURES | MODE_SAVE_CLASSES

                elif key in ('save-classes-only',):
                    env['mode'] = MODE_SAVE_CLASSES

                elif key in ('release',):
                    env['release'] = val

            if not env['classes']:
                print_usage()
            if env['max-classes'] and env['max-classes'] > len(env['classes']):
                print('Error: \'max-classes\' cannot be greater than number of classes specified.')
                print_usage()
        except:
            print_usage()


def print_usage(exit_code=1):
    print('===== USAGE =====\n'
          'imagenet-pull [-j PROCESSES_COUNT] [-t THREADS_PER_PROCESS] '
          '-c CLASSES [-R] [-C MAX_CLASSES] [-p PICTURES_PER_CLASS]\n'
          '[--jobs PROCESSES_COUNT] [--threads THREADS_PER_PROCESS] '
          '[--classes CLASSES] [--recursive] [--max-classes] '
          '[--pictures-per-class PICTURES_PER_CLASS] '
          '[--save-classes] [--save-classes-only] [--release IMAGENET_RELEASE]'
          '\n\n'
          '-R: recursive pull for classes (default False). E.g.: as class you specified \'nature\', '
          'if you set -R (or --recursive), the program will also pull all nested subclasses'
          'together with pictures of class \'nature\'.\n'
          '--save-classes: also saves all classes in a file under specified directory. '
          '(default False)\n'
          '--save-classes-only: only saves all classes without loading pictures (default False).\n'
          '\n'
          'PROCESSES_COUNT: number of parallel processes\n'
          'THREADS_PER_PROCESS: number of parallel threads per process\n'
          'CLASSES: class WNIDs separated by comma. E.g.: --classes="n00000001, n00000002, n00000003"\n'
          'PICTURES_PER_CLASS: maximum number of pictures per class\n')

    sys.exit(exit_code)


def main():
    global env

    # create context manager, set env as 'shared' dictionary and get arguments from shell (_set_args())
    manager = Manager()
    env = manager.dict()
    env['lock'] = manager.Lock()

    _set_args()

    class_worker = cd.ClassDistributer(env)
    all_classes = class_worker.get_env_classes()

    if env['mode'] & MODE_SAVE_CLASSES:
        classes_path = 'classes.txt'
        if env['dir']:
            classes_path = os.path.join(env['dir'], classes_path)
        fp = open(classes_path, mode='w')
        text = '\n'.join([f'{x[3]}; {x[0]}; {x[1]}; {x[2]}' for x in all_classes])
        fp.write(text)
        fp.close()

    n_jobs = env['n_jobs']
    jobs_classes = class_worker.get_classes_per_job(n_jobs, all_classes)
    proc_pool = Pool(processes=n_jobs)

    # parallel jobs
    if env['mode'] & MODE_LOAD_PICTURES:
        result = proc_pool.starmap_async(workers.main, [
            (env, jobs_classes[i], f'Process {i}')
            for i in range(n_jobs)
        ])
        proc_pool.close()

        # result.wait()
        # or
        proc_pool.join()


if __name__ == '__main__':
    main()
