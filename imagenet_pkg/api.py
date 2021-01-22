import imagenet_pkg.class_distributer as cd
import imagenet_pkg.images_puller as image_puller
import os
from imagenet_pkg.constants import *


class ApiSession:
    def __init__(self,
                 db_conn,
                 imagenet_release=DEFAULT_RELEASE,
                 images_dir='',
                 url_on_fail=URL_ON_FAIL_IGNORE,
                 max_async_requests=MAX_ASYNC_REQUESTS_DEFAULT):
        self.class_manager = cd.ClassDistributer(db_conn=db_conn,
                                                 release=imagenet_release,
                                                 max_async_requests=max_async_requests)
        self.imagenet_puller = image_puller.ImagesWorker(self.class_manager,
                                                         db_conn=db_conn,
                                                         directory=images_dir,
                                                         imagenet_release=imagenet_release,
                                                         url_on_fail=url_on_fail,
                                                         max_async_requests=max_async_requests)

        self._init = False

        self.images_dir = images_dir

    def __enter__(self):
        self.init()
        return self

    def __exit__(self):
        pass

    def _check_init(self):
        if not self._init:
            raise Exception('Imagenet API Session has not been initialized yet.')

    def init(self):
        self.class_manager.init_db()
        self._init = True

    def get_wnid_info(self, wnid, recursive=False, deep=None):
        """
        Returns list of (wnid, short_name, full_name, path) if argument "wnid" is a list of wnids. Or if
        argument "wnid" is a single string, returns (wnid, short_name, full_name, path)
        :param wnid: list of wnids or a single string
        :param recursive: defines wether to fetch childs
        :param deep: if "recursive" is set to true, defines how deep the hierarchy is
        :return: (wnid, short_name, full_name, path)
        """
        self._check_init()

        ret_one = isinstance(wnid, str)
        if ret_one:
            wnid = [wnid]
            ret_one = not recursive

        result = self.class_manager.get_classes_info(wnids=wnid,
                                                     recursive=recursive,
                                                     deep=deep)
        return result[0] if ret_one else result

    def get_wnids(self, parent=None, level=None):
        """
        Returns list of wnids. If none of arguments specified, returns all cached wnids.
        :param parent: can be not only the direct parent, but also the parent of parent etc.
        :param level: level in common hierarchy (where 0 is global)
        :return: List of wnids
        """
        self._check_init()
        wnids = self.class_manager.get_wnids(parent, level)
        if level:
            return [
                wnid for wnid in wnids
                if self.class_manager.levels[wnid] == level
            ]
        return wnids

    def cache_urls(self, wnids):
        self._check_init()
        return self.class_manager.cache_urls(wnids=wnids)

    def get_urls(self, wnid):
        """
        :param wnid: May be either single string or a list or strings
        :return: (url_id, wnid, url, state_id)
        """
        self._check_init()
        return self.class_manager.get_urls(wnid)

    def fetch(self, urls):
        """
        Downloads given urls
        :param urls: list of urls
        """
        self._check_init()
        return self.imagenet_puller.fetch(urls)

    def clean_all(self):
        self._check_init()
        self.class_manager.clean()
        self.imagenet_puller.clean()

    def clean_images(self):
        self._check_init()
        self.imagenet_puller.clean()

    def images_data_iter(self, wnid):
        self._check_init()
        list_files = os.listdir(os.path.join(self.images_dir, wnid))

        for file_name in list_files:
            if not file_name.lower().endswith('.jpeg'):
                continue
            file_name = os.path.join(self.images_dir, wnid, file_name)
            with open(file_name, mode='rb') as fp:
                yield fp.read()
