import os
import re
import itertools
import imagenet_pkg.util as util
from collections import defaultdict
from imagenet_pkg.constants import *
import psycopg2 as pspg
import psycopg2.extensions


class ClassDistributer:

    def __init__(self, db_conn, release=None, max_async_requests=MAX_ASYNC_REQUESTS_DEFAULT):
        # self.env = env
        self.words = None
        self.short_words = None
        self.parent_children = None
        self.child_parent = None
        self.paths = None
        self.levels = None

        self.db_conn: pspg.extensions.connection = db_conn
        self.db_cursor: pspg.extensions.cursor = self.db_conn.cursor()

        self.release = release
        self.max_async_requests = max_async_requests

    def _sql_insert(self, query):
        self.db_cursor.execute('ROLLBACK')
        self.db_cursor.execute(query)
        self.db_conn.commit()

    def _sql_select(self, query):
        self.db_cursor.execute(query)
        return self.db_cursor.fetchall()

    def _cache_data(self, datatype):
        """
        This function pulls data from image-net and caches into postgres database
        :param datatype: supported values: ('classes', 'hierarchy')
        :return:
        """

        if datatype == 'structure':
            url = IMGNETAPI_ALLHIERARCHY
            text = util.get(url).text.replace("'", "''")
            hierarchy = text.strip(' \n\t').split('\n')
            hierarchy = [x.split(' ') for x in hierarchy]

            query = f"INSERT INTO structure (release, parent_wnid, child_wnid) VALUES " + \
                    ','.join([f"('{self.release}', '{x[0]}', '{x[1]}') " for x in hierarchy]) + \
                    ' ON CONFLICT DO NOTHING;'
            self._sql_insert(query)
        elif datatype == 'classes':
            url = IMGNETAPI_ALLWORDS
            words = util.get(url).text.replace("'", "''")
            words = words.strip(' \n\t').split('\n')
            words = [tuple(str(w).split('\t')) for w in words]

            query = f'INSERT INTO classes (wnid, words) VALUES ' + \
                    ','.join([f"('{x[0]}', '{x[1]}') " for x in words]) + \
                    ' ON CONFLICT DO NOTHING;'
            self._sql_insert(query)
        elif datatype == 'hierarchy':
            data_str = [f"('{wnid}', '{self.paths[wnid]}')" for wnid in self.paths]
            query = f"INSERT INTO classes (wnid, hierarchy) VALUES " + \
                    ','.join(data_str) + \
                    " ON CONFLICT (wnid) DO UPDATE SET hierarchy = EXCLUDED.hierarchy;"
            self._sql_insert(query)
        else:
            raise Exception(f'Invalid datatype {datatype}')

    def _get_data(self, datatype):
        """
        This function pulls data from server if it wasn't pulled earlier and
        caches into database. Or if was pulled earlier, returns cached data.
        """
        if datatype == 'structure':
            self.db_cursor.execute(f'SELECT COUNT(*) FROM structure WHERE release = \'{self.release}\';')
            cnt = self.db_cursor.fetchone()[0]
            if not cnt:
                self._cache_data(datatype)
            query = f'SELECT parent_wnid, child_wnid FROM structure WHERE release = \'{self.release}\';'
            hierarchy = self._sql_select(query)
            return hierarchy
        elif datatype == 'classes':
            self.db_cursor.execute(f'SELECT COUNT(*) FROM classes;')
            cnt = self.db_cursor.fetchone()[0]
            if not cnt:
                self._cache_data(datatype)
            query = f'SELECT wnid, words FROM classes;'
            classes = self._sql_select(query)
            return classes
        else:
            raise Exception(f'Invalid datatype {datatype}')

    def _get_classes_info(self, wnids):
        """
        This function returns array of (wnid, short_name, full_name, path).
        If 'recursive' is True, either returns all childs attributes, if False,
        returns just (wnid, short_name, full_name, path)
        """

        if isinstance(wnids, str):
            wnid = wnids
            return wnid, self.short_words[wnid], self.words[wnid], self.paths[wnid]

        return [
            (wnid, self.short_words[wnid], self.words[wnid], self.paths[wnid])
            for wnid in wnids
        ]

    def _set_classes_db(self):
        def _get_path_of(wnid):
            # if it has a parent
            if wnid in self.child_parent:
                parent = self.child_parent[wnid]
                return os.path.join(_get_path_of(parent), wnid)
            return wnid

        def _get_paths():
            return {wnid: _get_path_of(wnid) for wnid in self.words}

        # first get classes
        classes = self._get_data('classes')
        structure = self._get_data('structure')

        self.wnids = [x[0] for x in classes]  # where x[0] is wnid
        self.words = {x[0]: x[1] for x in classes}
        self.short_words = {wnid: re.split(r'(\s*,\s*)+', self.words[wnid])[0]
                            for wnid in self.words}

        gb = defaultdict(list)
        # row[0] is parent_wnid, row[1] is child_wnid
        for row in structure:
            gb[row[0]].append(row[1])

        self.parent_children = {key: gb[key] for key in gb}
        self.child_parent = {x[1]: x[0] for x in structure}

        self.paths = _get_paths()
        self.levels = {
            wnid: len(re.findall(r'n\d{8}', self.paths[wnid]))
            for wnid in self.paths
        }

        query = f"SELECT COUNT(*) FROM classes WHERE hierarchy IS NULL;"
        cnt = self._sql_select(query)[0][0]
        if cnt:
            self._cache_data('hierarchy')

    def _is_cached(self, wnid):
        query = f'SELECT 1 FROM urls ' \
                f'WHERE release = \'{self.release}\' ' \
                f'  AND wnid = \'{wnid}\' ' \
                f'LIMIT 1'
        self.db_cursor.execute(query)
        cnt = self.db_cursor.fetchone()
        return cnt > 0

    def _get_list_cached_urls_wnids(self):
        query = f'SELECT DISTINCT wnid FROM urls WHERE release = \'{self.release}\';'
        wnids = list(itertools.chain.from_iterable(self._sql_select(query)))  # get flatten list of wnids
        return set(wnids)

    def init_db(self, debug=False):
        if debug:
            print('Initializing data...')
        self._set_classes_db()

    def cache_urls(self, wnids):
        """
        Checks if passed wnids are not already cached. Caches if not.
        """
        cached = self._get_list_cached_urls_wnids()
        urls = [(IMGNETAPI_URLS.format(wnid), wnid) for wnid in wnids if wnid not in cached]

        stats = {
            'loaded': 0,
            'total': len(urls)
        }

        def print_stats():
            print(f'\r[LOADING URLS] [LOADED/TOTAL] {stats["loaded"]}/{stats["total"]}', end='')

        def on_fetch(response):
            (url, wnid), data = response
            urls_list_str = [(wnid, u.replace("'", "''")) for u in re.split(r'[\n\r]+', data.decode('utf-8'))]

            query = f"INSERT INTO urls (release, wnid, url) VALUES " + \
                    ','.join([
                        f"('{self.release}', '{wnid}', '{url}')" for wnid, url in urls_list_str
                    ]) + f" ON CONFLICT DO NOTHING;"
            self._sql_insert(query)

            query = f"INSERT INTO url_states (url_id, state_id) " \
                    f"    (SELECT url.id as url_id, 1 as state_id FROM urls url " \
                    f"     WHERE url.wnid = '{wnid}') " \
                    f"ON CONFLICT (url_id) DO UPDATE SET state_id = EXCLUDED.state_id;"
            self._sql_insert(query)

            stats["loaded"] += 1

            print_stats()

        util.fetch_with_callback(urls, on_fetch, on_fail=URL_ON_FAIL_RETRY, async_limit=self.max_async_requests)

    def get_classes_info(self, wnids, recursive=False, deep=None):
        """
        Returns array of (wnid, short_name, full_name, path)
        :param wnids: list of wnids
        :param recursive: if True, returns also info of children of "wnids"
        :param deep: if "recursive" is True, defines how deep the hierarchy of children is
        :return: array of (wnid, short_name, full_name, path)
        """
        # get_wnids returns array of (wnid, short_name, full_name, path)
        if recursive:
            childs = [self.get_wnids(parent=wnid, deep=deep) for wnid in wnids]
            childs = list(itertools.chain.from_iterable(childs))

            # since get_wnids() doesn't include parent, add to list beginning
            wnids = wnids + childs
        return self._get_classes_info(wnids)

    def get_wnids(self, parent=None, deep=None):
        """
        Returns list of wnids
        :param parent: if specified, list of "parent"s childred returned.
        Note that "parent" is not returned within the list.
        :param deep: if "parent" specified, "deep" specifies how deep hierarchy of children
        :return: list of wnids
        """
        def _get_childs(parent, deep, level):
            if parent not in self.parent_children:
                return []

            childs = self.parent_children[parent]

            if not deep or level < deep:
                for child in childs:
                    childs = childs + _get_childs(child, deep, level + 1)

            return childs

        if parent:
            return _get_childs(parent, deep, 1)

        return self.wnids

    def get_urls(self, wnid):
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
            self.cache_urls(wnids)  # this function checks cache state of given wnids.

        wnids_str = [f"'{wnid}'" for wnid in wnids]
        query = f"SELECT url.id, url.wnid, url.url, ust.state_id " \
                f"FROM urls url " \
                f"     LEFT OUTER JOIN url_states ust " \
                f"          ON ust.url_id = url.id " \
                f"WHERE ust.state_id not in (3, 4)" \
                f"  AND url.wnid in ({','.join(wnids_str)})" \
                f"ORDER BY ust.state_id, url.wnid;"  # state_id(4) - downloaded, state_id(3) - not jpeg
        data = self._sql_select(query)

        return data

    def clean(self):
        queries = [
            f"TRUNCATE url_states CASCADE;",
            f"TRUNCATE urls CASCADE;",
            f"TRUNCATE structure CASCADE;",
            f"TRUNCATE classes CASCADE;"
        ]
        for query in queries:
            self._sql_insert(query)
