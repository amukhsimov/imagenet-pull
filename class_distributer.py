import requests
import urllib3
import os
import re
import time
import itertools
import threading
import multiprocessing as mp
import numpy as np
from collections import defaultdict
from constants import *

MAX_ASYNC_REQUESTS = 8
MAX_RECONNECT_ATTEMPTS = 3


class ClassDistributer:

    def __init__(self, env):
        self.env = env

        self._set_classes_db()

    def _get(self, get):
        response = False
        connect_attemp = 0
        result = ''
        while not response:
            connect_attemp = connect_attemp + 1
            # if retry count exceeded, return empty string
            if connect_attemp > MAX_RECONNECT_ATTEMPTS + 1:
                break
            try:
                result = requests.get(get).text
                response = True
            except urllib3.exceptions.MaxRetryError as ex:
                time.sleep(5)
                continue
            except:
                continue

        return result

    def _get_file(self, filetype, release):
        """
        This function pulls data from server if it wasn't pulled earlier and
        caches it in specified directory. Or if it was pulled before, returns cached file.
        """
        path = self.env['dir']
        filename = f'{filetype}-{release}.bin'

        files = os.listdir(path if path else '.')
        fullname = os.path.join(path, filename)

        if filename in files:
            with open(fullname, 'r') as fp:
                data = fp.read()
            return data

        url = None
        if filetype == 'hierarchy':
            url = IMGNETAPI_ALLHIERARCHY
        elif filetype == 'words':
            url = IMGNETAPI_ALLWORDS
        else:
            raise Exception(f'Invalid argument: filetype - {filetype}')

        filetext = self._get(url)
        with open(fullname, 'wb') as fp:
            fp.write(bytes(filetext, encoding='utf-8'))

        return filetext

    def _get_class_name(self, wnid):
        word = self.words[wnid]
        # return either full name and short name (short name is just the first name)
        return word, re.split(r'(\s*,\s*)+', word)[0]

    def _get_child_wnids(self, wnid_parent):
        get = IMGNETAPI_CHILDS.format(wnid_parent)
        text = self._get(get)

        wnids = re.findall(r'-(n\d{8})', text)

        return wnids

    def _get_class_attributes(self, wnid, recursive=False):
        full_name, short_name = self._get_class_name(wnid)
        # if for some reason no data retrieved, return empty array
        if not full_name:
            return []

        short_name = self.short_words[wnid]
        path = self.paths[wnid]

        child_wnids = None
        if wnid in self.parent_children:
            child_wnids = self.parent_children[wnid]

        # recursive get names for child classes
        # all child classes are returned in one flatten list
        # and if no child classes wnids, returned only current one, also in list
        if recursive and child_wnids:
            child_classes = [self._get_class_attributes(c_wnid, True) for c_wnid in child_wnids]
            child_classes = itertools.chain.from_iterable(child_classes)
            child_classes = list(child_classes)
        else:
            child_classes = []

        return [(wnid, short_name, full_name, path)] + child_classes

    def _set_classes_db(self):
        def _get_path_of(wnid):
            # if it has a parent
            if wnid in self.child_parent:
                parent = self.child_parent[wnid]
                return os.path.join(_get_path_of(parent), self.short_words[wnid])
            return self.short_words[wnid]

        def _get_paths():
            return {wnid: _get_path_of(wnid) for wnid in self.words}

        print('Fetching classes DB...')
        # raw
        all_hierarchy = self._get_file('hierarchy', self.env['release'])
        all_words = self._get_file('words', self.env['release'])

        all_words = all_words.strip(' \n\t').split('\n')
        all_words = [tuple(str(w).split('\t')) for w in all_words]

        self.words = {x[0]: x[1] for x in all_words}
        self.short_words = {wnid: re.split(r'(\s*,\s*)+', self.words[wnid])[0]
                            for wnid in self.words}

        all_hierarchy = all_hierarchy.strip(' \n\t').split('\n')
        all_hierarchy = [x.split(' ') for x in all_hierarchy]
        gb = defaultdict(list)
        for row in all_hierarchy:
            gb[row[0]].append(row[1])

        self.parent_children = {key: gb[key] for key in gb}
        self.child_parent = {x[1]: x[0] for x in all_hierarchy}

        self.paths = _get_paths()

        print('Classes DB was successfully set.')

    def get_env_classes(self):
        """
        Returns array type of (wnid, short_name, full_name, path), where
        full_label is a string including all parent labels (made for folding)
        :param env:
        """

        print('Fetching all classes...')

        lst = [self._get_class_attributes(
            class_wnid,
            self.env['recursive']
        ) for class_wnid in self.env['classes']]

        classes = list(itertools.chain.from_iterable(lst))

        classes = sorted(classes, key=lambda x: x[3])

        return classes

    def get_classes_per_job(self, n_jobs, classes=None):
        if not classes:
            classes = self.get_env_classes()

        classes = np.array(classes)

        classes_per_job = classes // n_jobs
        indices = np.random.permutation(classes)
        result = []
        for i in range(n_jobs):
            i_start = i * classes_per_job
            i_end = len(classes) if i == n_jobs - 1 else (i + 1) * classes_per_job
            result.append(list(classes[indices[i_start:i_end]]))

        return result
