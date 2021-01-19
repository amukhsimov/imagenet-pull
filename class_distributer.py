import requests
import urllib3
import os
import re
import time
import itertools
import threading
import multiprocessing as mp
import numpy as np
import util
from collections import defaultdict
from constants import *
import json
import psycopg2 as pspg
import psycopg2.extensions


class ClassDistributer:

    def __init__(self, env):
        self.env = env
        self.words = None
        self.short_words = None
        self.parent_children = None
        self.child_parent = None
        self.paths = None

        self.db_conn: pspg.extensions.connection = env['db_conn']
        self.db_cursor: pspg.extensions.cursor = self.db_conn.cursor()

    def _sql_insert(self, query):
        self.db_cursor.execute('ROLLBACK')
        self.db_cursor.execute(query)
        self.db_conn.commit()

    def _sql_select(self, query):
        self.db_cursor.execute(query)
        return self.db_cursor.fetchall()

    def _cache_data(self, datatype, release=None):
        """
        This function pulls data from image-net and caches into postgres database
        :param datatype: supported values: ('classes', 'hierarchy')
        :param release: image-net release. If none, took from self.env
        :return:
        """
        if not release:
            release = self.env['release']

        if datatype == 'hierarchy':
            url = IMGNETAPI_ALLHIERARCHY
            text = util.get(url).text.replace("'", "''")
            hierarchy = text.strip(' \n\t').split('\n')
            hierarchy = [x.split(' ') for x in hierarchy]

            query = f"INSERT INTO structure (release, parent_wnid, child_wnid) VALUES " + \
                    ','.join([f"('{release}', '{x[0]}', '{x[1]}') " for x in hierarchy]) + \
                    'ON CONFLICT DO NOTHING;'
            self._sql_insert(query)
        elif datatype == 'classes':
            url = IMGNETAPI_ALLWORDS
            words = util.get(url).text.replace("'", "''")
            words = words.strip(' \n\t').split('\n')
            words = [tuple(str(w).split('\t')) for w in words]

            query = f'INSERT INTO classes (wnid, words) VALUES ' + \
                    ','.join([f"('{x[0]}', '{x[1]}') " for x in words]) + \
                    'ON CONFLICT DO NOTHING;'
            self._sql_insert(query)
        else:
            raise Exception(f'Invalid datatype {datatype}')

    def _get_data(self, datatype, release):
        """
        This function pulls data from server if it wasn't pulled earlier and
        caches into database. Or if was pulled earlier, returns cached data.
        """
        if datatype == 'hierarchy':
            self.db_cursor.execute(f'SELECT COUNT(*) FROM structure WHERE release = \'{release}\';')
            cnt = self.db_cursor.fetchone()[0]
            if not cnt:
                self._cache_data(datatype, release)
            query = f'SELECT parent_wnid, child_wnid FROM structure WHERE release = \'{release}\';'
            hierarchy = self._sql_select(query)
            return hierarchy
        elif datatype == 'classes':
            self.db_cursor.execute(f'SELECT COUNT(*) FROM classes;')
            cnt = self.db_cursor.fetchone()[0]
            if not cnt:
                self._cache_data(datatype, release)
            query = f'SELECT wnid, words FROM classes;'
            classes = self._sql_select(query)
            return classes
        else:
            raise Exception(f'Invalid datatype {datatype}')

    def _get_class_attributes(self, wnid, recursive=False):
        """
        This function returns array of (wnid, short_name, full_name, path).
        If 'recursive' is True, either returns all childs attributes, if False,
        returns just [(wnid, short_name, full_name, path)]
        """
        full_name, short_name, path = self.words[wnid], self.short_words[wnid], self.paths[wnid]

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

        # first get classes
        classes = self._get_data('classes', self.env['release'])
        hierarchy = self._get_data('hierarchy', self.env['release'])

        self.words = {x[0]: x[1] for x in classes}
        self.short_words = {wnid: re.split(r'(\s*,\s*)+', self.words[wnid])[0]
                            for wnid in self.words}

        gb = defaultdict(list)
        # row[0] is parent_wnid, row[1] is child_wnid
        for row in hierarchy:
            gb[row[0]].append(row[1])

        self.parent_children = {key: gb[key] for key in gb}
        self.child_parent = {x[1]: x[0] for x in hierarchy}

        self.paths = _get_paths()

    def _is_cached(self, wnid, release=None):
        if not release:
            release = self.env['release']
        query = f'SELECT 1 FROM urls ' \
                f'WHERE release = \'{release}\' ' \
                f'  AND wnid = \'{wnid}\' ' \
                f'LIMIT 1'
        self.db_cursor.execute(query)
        cnt = self.db_cursor.fetchone()
        return cnt > 0

    def _get_list_cached_urls_wnids(self, release=None):
        if not release:
            release = self.env['release']
        query = f'SELECT DISTINCT wnid FROM urls WHERE release = \'{release}\';'
        wnids = list(itertools.chain.from_iterable(self._sql_select(query)))  # get flatten list of wnids
        return set(wnids)

    def init_db(self, debug=False):
        if debug:
            print('Initializing data...')
        self._set_classes_db()

    def cache_urls(self, wnids, min_valid_ratio=0.95, debug=False):
        cached = self._get_list_cached_urls_wnids()
        urls = [(IMGNETAPI_URLS.format(wnid), wnid) for wnid in wnids if wnid not in cached]

        valid_responses_count, total_urls_count = 0, len(urls)
        pending_responses = urls.copy()
        # if some urks had no response the first time, try again.
        # number of reconnections is set in _set_args() function of main module.
        # connection timeout is also set in _set_args()
        while len(pending_responses) > (1 - min_valid_ratio) * len(urls):
            # response's structure is: if success, (url, wnid, data), otherwise (url, wnid)
            responses = util.get_async(pending_responses[:MAX_ASYNC_IMAGENET_REQUESTS], timeout=10)
            assert not any([x for x in responses if len(x) != 3 and len(x) != 2])

            valid_responses = [x for x in responses if len(x) == 3]
            valid_responses_count += len(valid_responses)

            valid_urls = [(wnid, re.split(r'[\n\r]+', data.decode('utf-8'))) for url, wnid, data in valid_responses]
            valid_urls = [[(wnid, url) for url in urls_list] for wnid, urls_list in valid_urls]
            valid_urls = list(itertools.chain.from_iterable(valid_urls))  # here a flatten array of (wnid, url)

            # insert urls into master table
            valid_urls_text = [(wnid, url.replace("'", "''")) for wnid, url in valid_urls]
            if valid_urls_text:
                query = f"INSERT INTO urls (release, wnid, url) VALUES " + \
                        ','.join([
                            f"('{self.env['release']}', '{wnid}', '{url}') " for wnid, url in valid_urls_text
                        ]) + f"ON CONFLICT DO NOTHING;"
                self._sql_insert(query)

                # update url states
                wnids_str = set([f"'{wnid}'" for url, wnid, data in valid_responses])
                query = f"INSERT INTO url_states (url_id, state_id) " \
                        f"    (SELECT url.id as url_id, 1 as state_id FROM urls url " \
                        f"     WHERE url.wnid in ({','.join(wnids_str)})) " \
                        f"ON CONFLICT (url_id) DO UPDATE SET state_id = EXCLUDED.state_id;"
                self._sql_insert(query)

            pending_responses = pending_responses[MAX_ASYNC_IMAGENET_REQUESTS:] + [x for x in responses if len(x) == 2]
            if debug:
                print(f'Loading classes URLs: [LOADED/TOTAL] {valid_responses_count}/{total_urls_count}')

    def get_env_classes(self):
        """
        Returns array of structure (wnid, short_name, full_name, path)
        """
        # _get_class_attributes returns array of (wnid, short_name, full_name, path)
        lst = [self._get_class_attributes(
            class_wnid,
            self.env['recursive']
        ) for class_wnid in self.env['classes']]

        classes = list(itertools.chain.from_iterable(lst))

        classes = sorted(classes, key=lambda x: x[3])

        return classes

    def get_urls_of(self, wnid):
        """
        returns array of (url_id, wnid, url, state_id)
        :param wnid:
        :return:
        """

        if isinstance(wnid, str):
            wnids = [wnid]
            if not self._is_cached(wnid):
                self.cache_urls([wnid])
        else:
            wnids = wnid
            self.cache_urls(wnids, 1)

        wnids_str = [f"'{wnid}'" for wnid in wnids]
        query = f"SELECT url.id, url.wnid, url.url, ust.state_id " \
                f"FROM urls url " \
                f"     LEFT OUTER JOIN url_states ust " \
                f"          ON ust.url_id = url.id " \
                f"WHERE ust.state_id != 4" \
                f"  AND url.wnid in ({','.join(wnids_str)});"  # state_id(4) - downloaded
        data = self._sql_select(query)

        return data

    def get_class_meta(self, wnid):
        """
        returns (wnid, shortname, fullname, path)
        :param wnid:
        :return:
        """
        return wnid, self.short_words[wnid], self.words[wnid], self.paths[wnid]

    def clean(self):
        queries = [
            f"TRUNCATE url_states CASCADE;",
            f"TRUNCATE urls CASCADE;",
            f"TRUNCATE structure CASCADE;",
            f"TRUNCATE classes CASCADE;"
        ]
        for query in queries:
            self._sql_insert(query)
