import time
import requests
import threading
import numpy as np
import os
import multiprocessing as mp
import hashlib
from constants import *
import class_distributer as cd

_env = None


def _sha256(text):
    hashlib.sha256(bytes(text, 'utf-8')).hexdigest()


def _worker_main(env, ):
    pass


def _get_loadable_classes():
    pass


def main(env, classes, proc_name):
    global _env
    _env = env
    n_threads = env['threads-per-job']

    pool = mp.pool.ThreadPool(n_threads)
    result_async = pool.starmap_async(_worker_main, [(env,) for i in range(n_threads)])

    pool.close()
    pool.join()
