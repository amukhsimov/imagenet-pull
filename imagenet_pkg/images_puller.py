import os
import hashlib
from imagenet_pkg.constants import *
from imagenet_pkg.class_distributer import ClassDistributer
import imagenet_pkg.util as util
import imghdr
import psycopg2 as pspg
import psycopg2.extensions
import shutil
import re


class ImagesWorker:
    def __init__(self, class_manager, db_conn, directory, imagenet_release=DEFAULT_RELEASE,
                 url_on_fail=URL_ON_FAIL_IGNORE, max_async_requests=MAX_ASYNC_REQUESTS_DEFAULT):
        self.class_manager: ClassDistributer = class_manager
        self.db_conn: pspg.extensions.connection = db_conn
        self.cursor: pspg.extensions.cursor = self.db_conn.cursor()
        self.directory = directory

        self.release = imagenet_release
        self.url_on_fail = url_on_fail
        self.max_async_requests = max_async_requests

    def _sha256(self, text):
        return hashlib.sha256(bytes(text, 'utf-8')).hexdigest()

    def _save_images(self, data):
        """
        :param data: list of (url, url_id, wnid, bytes)
        :return: dictionary of url_id: (url_id, wnid, url, state)
        """

        # classes valid images counts
        result = {}
        for url, url_id, wnid, img_bytes in data:
            # if bytes are a valid jpeg image
            if imghdr.what(None, img_bytes) == 'jpeg':
                # get class metadata
                wnid, shortname, fullname, path = self.class_manager.get_classes_info([wnid])[0]
                directory = os.path.join(self.directory, wnid)
                # create class directory if it doesn't exist
                if not os.path.isdir(directory):
                    os.makedirs(directory)
                # to avoid name conflicts, set hash as name
                filename = self._sha256(url)[:20] + '.jpg'
                with open(os.path.join(directory, filename), 'wb') as fp:
                    fp.write(img_bytes)
                result[url_id] = (url_id, wnid, url, 4)  # where 4 is 'downloaded' state
            else:
                result[url_id] = (url_id, wnid, url, 3)  # where 3 is 'invalid data' state

        return result

    def _save_states(self, states):
        """
        :param states: an array of (url_id, state_id)
        :return:
        """
        data_str = [f"({url_id}, {state_id})" for url_id, state_id in states]
        query = f"INSERT INTO url_states (url_id, state_id) VALUES " \
                f"{','.join(data_str)} " \
                f"ON CONFLICT (url_id) DO UPDATE SET state_id = EXCLUDED.state_id;"

        if data_str:
            self.cursor.execute('ROLLBACK')
            self.cursor.execute(query)
            self.db_conn.commit()

    def _fetch(self, urls):
        """
        :param urls: a dictionary of (url_id, wnid, url, state_id)
        :return:
        """

        stats = {
            'saved': 0,
            'fetched': 0,
            'failed': 0,
            'total': len(urls)
        }

        print(f'Start fetching {len(urls)} urls...')

        def print_stats():
            print(f'\r[SAVED/FETCHED/FAILED/TOTAL] '
                  f'{stats["saved"]}/{stats["fetched"]}/{stats["failed"]}/{stats["total"]}', end='')

        def on_fetch(response):
            # response is ((url, url_id, wnid), data)
            stats['fetched'] += 1
            (url, url_id, wnid), data = response

            save_state = self._save_images([(url, url_id, wnid, data)])
            url_id, wnid, url, state = save_state[url_id]

            self._save_states([(url_id, state)])

            if state == 4:
                stats['saved'] += 1

            print_stats()

        def on_fail(response):
            # response is (url, url_id, wnid)
            stats['failed'] += 1
            url, url_id, wnid = response

            self._save_states([(url_id, 2)])  # where 2 is 'failed'

            print_stats()

        urls = [(url, url_id, wnid) for url_id, wnid, url, state_id in urls]
        util.fetch_with_callback(urls,
                                 on_fetch,
                                 on_fail,
                                 on_fail=self.url_on_fail,
                                 async_limit=self.max_async_requests)

    def fetch(self, urls):
        """
        :param urls: a dictionary of (url_id, wnid, url, state_id)
        :return:
        """
        self._fetch(urls)

    def clean(self):
        if os.path.isdir(self.directory):
            lst_files = os.listdir(self.directory)
            for folder in lst_files:
                if re.fullmatch(r'n\d{8}', folder):
                    shutil.rmtree(os.path.join(self.directory, folder))

        query = f"UPDATE url_states SET state_id = 1"
        self.cursor.execute('ROLLBACK;')
        self.cursor.execute(query)
        self.db_conn.commit()
