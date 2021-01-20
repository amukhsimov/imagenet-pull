import time
import requests
import threading
import numpy as np
import os
import multiprocessing as mp
import hashlib
from collections import defaultdict
from constants import *
import class_distributer as cd
from class_distributer import ClassDistributer
import util
import itertools
import imghdr
import psycopg2 as pspg
import psycopg2.extensions
import shutil


class ImagesWorker:
    def __init__(self, env):
        self.env = env
        self.class_worker: ClassDistributer = env['class_manager']
        self.db_conn: pspg.extensions.connection = env['db_conn']
        self.cursor: pspg.extensions.cursor = self.db_conn.cursor()

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
                wnid, shortname, fullname, path = self.env['class_manager'].get_class_meta(wnid)
                directory = os.path.join(self.env['dir'], 'images', path)
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

    def _fetch_worker(self, urls, debug=False):
        """
        :param urls: a dictionary of (url_id, wnid, url, state_id)
        :return:
        """

        # valid_images_count, total_urls_count = 0, len(urls)
        valid_images, success_responses, failed_responses, total_urls = 0, 0, 0, len(urls)
        stats = {
            'saved': 0,
            'fetched': 0,
            'failed': 0,
            'total': len(urls)
        }

        if debug:
            print(f'Start fetching {len(urls)} urls...')

        ratio = self.env['fetch_ratio']

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
                                 on_fail=URL_ON_FAIL_RETRY,
                                 async_limit=MAX_ASYNC_REQUESTS)

    def fetch(self, urls, debug=False):
        """
        :param urls: a dictionary of (url_id, wnid, url, state_id)
        :return:
        """
        self._fetch_worker(urls, debug)

    def clean(self):
        directory = os.path.join(self.env['dir'], 'images')
        if os.path.isdir(directory):
            shutil.rmtree(directory)
        query = f"UPDATE url_states SET state_id = 1"
        self.cursor.execute('ROLLBACK;')
        self.cursor.execute(query)
        self.db_conn.commit()
