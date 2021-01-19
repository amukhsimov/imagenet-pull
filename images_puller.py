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
        :param data: list of (url, (url_id, wnid), bytes)
        :return: dictionary of counts of valid images grouped by wnid
        """

        # classes valid images counts
        result = {}
        for url, (url_id, wnid), img_bytes in data:
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
                result[url_id] = [url_id, wnid, url, 4]  # where 4 is 'downloaded' state
            else:
                result[url_id] = [url_id, wnid, url, 3]  # where 3 is 'invalid data' state

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

        self.cursor.execute('ROLLBACK')
        self.cursor.execute(query)
        self.db_conn.commit()

    def _fetch_worker(self, urls, debug=False):
        """
        :param urls: a dictionary of (url_id, wnid, url, state_id)
        :return:
        """

        valid_images_count, fetched_urls_count, total_urls_count = 0, 0, len(urls)

        if debug:
            print(f'Start fetching {len(urls)} urls...')

        ratio = self.env['fetch_ratio']

        # try:
        pending_responses = [(url, (url_id, wnid)) for url_id, wnid, url, state_id in urls]

        while fetched_urls_count < ratio * total_urls_count:
            cur_requests = pending_responses[:MAX_ASYNC_REQUESTS]
            # 'responses' is a list of (url, (url_id, wnid), bytes)
            responses = util.get_async(cur_requests, timeout=10)
            valid_responses = [x for x in responses if len(x) == 3]
            # pending means failed
            failed_responses = [x for x in responses if len(x) == 2]
            pending_responses = pending_responses[MAX_ASYNC_REQUESTS:] + failed_responses

            assert not any([x for x in responses if len(x) != 2 and len(x) != 3])

            # save pending_requests urls as 'unavailable' state
            self._save_states([(url_id, 2) for url, (url_id, wnid) in failed_responses])

            fetched_urls_count += len(valid_responses)

            # 'save_response' is a dictionary of url_id: [url_id, wnid, url, state_id]
            # 'valid_responses' is a list of (url, (url_id, wnid), bytes)
            save_response = self._save_images(valid_responses)
            save_response = list(save_response.values())
            self._save_states([(url_id, state_id) for url_id, wnid, url, state_id in save_response])

            total_saved = sum([
                1 for resp in save_response if resp[3] == 4  # where resp[3] is state_id (and 4 is 'downloaded')
            ])
            valid_images_count += total_saved

            if debug:
                print(f'[VALID/FETCHED/TOTAL] {valid_images_count}/{fetched_urls_count}/{total_urls_count}')
        # except Exception as ex:
        #     if debug:
        #         print(f'[EXCEPTION WHILE FETCHING] {ex}')

    def fetch(self, urls, debug=False):
        """
        :param urls: a dictionary of (url_id, wnid, url, state_id)
        :return:
        """

        thread = threading.Thread(target=self._fetch_worker, args=(urls, debug))
        thread.start()
        thread.join()

    def clean(self):
        directory = os.path.join(self.env['dir'], 'images')
        if os.path.isdir(directory):
            shutil.rmtree(directory)
        query = f"UPDATE url_states SET state_id = 1"
        self.cursor.execute('ROLLBACK;')
        self.cursor.execute(query)
        self.db_conn.commit()
