import sys
import getopt
import re
from imagenet_pkg.constants import *
import psycopg2 as pspg
import psycopg2.extensions
from imagenet_pkg.api import ApiSession

env = {}


def _set_args():
    env['classes'] = None
    env['recursive'] = False
    env['deep'] = None
    env['max-classes'] = None
    # env['pictures-per-class'] = None
    env['dir'] = ''
    env['mode'] = MODE_LOAD_PICTURES
    env['fetch_ratio'] = 0.8
    env['release'] = 'fall2011'
    env['max-async-requests'] = MAX_ASYNC_REQUESTS_DEFAULT

    env['pg_host'] = None
    env['pg_port'] = None
    env['pg_user'] = None
    env['pg_password'] = None
    env['pg_dbname'] = None

    if len(sys.argv) > 1:
        try:
            args = getopt.getopt(sys.argv[1:], 'hc:RC:p:d:r:v:m:n:', [
                'help',
                'usage',
                'pg_host=',
                'pg_port=',
                'pg_user=',
                'pg_password=',
                'pg_dbname=',
                'classes=',
                'recursive',
                'fetch-ratio=',
                'deep=',
                'max-classes=',
                # 'pictures-per-class=',
                'dir=',
                'mode=',
                'release=',
                'max-async-requests=',
            ])
            print(args[0])

            for key, val in args[0]:
                key = str(key).lstrip('-')

                if key in ('usage', 'help', 'h'):
                    print_usage(0)

                elif key in ('pg_host',):
                    env['pg_host'] = val

                elif key in ('pg_port',):
                    env['pg_port'] = val

                elif key in ('pg_user',):
                    env['pg_user'] = val

                elif key in ('pg_password',):
                    env['pg_password'] = val

                elif key in ('pg_dbname',):
                    env['pg_dbname'] = val

                elif key in ('classes', 'c'):
                    if re.fullmatch(r'(\s*n\d{8}\s*,?)+', val):
                        env['classes'] = re.findall(r'n\d{8}', val)
                    else:
                        print_usage()

                # elif key in ('pictures-per-class', 'p'):
                #     env['pictures-per-class'] = int(val)

                elif key in ('dir', 'd'):
                    env['dir'] = val

                elif key in ('recursive', 'R'):
                    env['recursive'] = True

                elif key in ('deep',):
                    env['deep'] = int(val)

                elif key in ('max-classes', 'C'):
                    env['max-classes'] = int(val)

                elif key in ('release', 'v'):
                    env['release'] = val

                elif key in ('fetch-ratio', 'r'):
                    env['fetch_ratio'] = float(val)

                elif key in ('max-async-requests', 'n'):
                    env['max-async-requests'] = int(val)

                elif key in ('mode', 'm'):
                    if val == 'urls':
                        env['mode'] = MODE_CACHE_URLS
                    elif val == 'images':
                        env['mode'] = MODE_LOAD_PICTURES
                    elif val == 'clear':
                        env['mode'] = MODE_CLEAR_CACHE
                    elif val == 'clear-images':
                        env['mode'] = MODE_CLEAR_IMAGES
                    else:
                        print_usage()

            if not env['classes'] and env['mode'] != MODE_CLEAR_CACHE and env['mode'] != MODE_CLEAR_IMAGES:
                print_usage()
            if env['max-classes'] and env['max-classes'] > len(env['classes']):
                print('Error: \'max-classes\' cannot be greater than number of classes specified.')
                print_usage()
        except Exception as ex:
            print(ex)
            print_usage()


def print_usage(exit_code=1):
    print('===== USAGE =====\n'
          'imagenet-pull '
          '-c CLASSES '
          '-d IMAGES_DIRECTORY '
          '[-r FETCH_RATIO] '
          '[-R] '
          '[-C MAX_CLASSES] '
          # '[-p PICTURES_PER_CLASS] '
          '[-v IMAGENET_RELEASE] '
          '[-n MAX_ASYNC_REQUESTS] '
          '[-m MODE]\n'
          '--pg_host POSTGRES_HOST '
          '--pg_port POSTGRES_PORT '
          '--pg_dbname POSTGRES_DBNAME '
          '--pg_user POSTGRES_USER '
          '--pg_password POSTGRES_PASSWORD '
          '--classes CLASSES '
          '--dir IMAGES_DIRECTORY '
          '[--fetch-ratio FETCH_RATIO] '
          '[--recursive] '
          '[--deep RECURSIVITY_DEEP] '
          '[--max-classes] '
          # '[--pictures-per-class PICTURES_PER_CLASS] '
          '[--max-async-requests MAX_ASYNC_REQUESTS] '
          '[--mode MODE] '
          '[--release IMAGENET_RELEASE]\n'
          '\n'
          '-R: recursive pull for classes (default False). E.g.: as class you specified \'nature\', '
          'if you set -R (or --recursive), the program will also pull all nested subclasses'
          'together with pictures of class \'nature\'.\n'
          '\n'
          'CLASSES: class WNIDs separated by comma. E.g.: --classes="n00000001, n00000002, n00000003"\n'
          'FETCH_RATIO: for example given n classes, which totally contains m urls, FETCH_RATIO tells, '
          'we have to fetch at least FETCH_RATIO * m images (default 0.8)\n'
          'RECURSIVITY_DEEP: hierarchy deepness\n'
          'MAX_ASYNC_REQUESTS: how many requests may be executing simultaneously. '
          f'Default {MAX_ASYNC_REQUESTS_DEFAULT}.\n'
          # 'PICTURES_PER_CLASS: maximum number of pictures per class\n'
          'IMAGENET_RELEASE: default fall2011\n'
          'MODE: set the mode. If MODE=urls, downloads urls, else if MODE=images, downloads images, '
          'else if MODE=clear, clears database, else if MODE=clear-images, clears images only. '
          'Default \'MODE=images\'\n')
    sys.exit(exit_code)


def main():
    global env

    # create context manager, set env as 'shared' dictionary and get arguments from shell (_set_args())

    _set_args()

    env['db_conn'] = pspg.connect(f'host={env["pg_host"]} '
                                  f'port={env["pg_port"]} '
                                  f'dbname={env["pg_dbname"]} '
                                  f'user={env["pg_user"]} '
                                  f'password={env["pg_password"]}')

    with ApiSession(env['db_conn'],
                    images_dir=env['dir'],
                    url_on_fail=URL_ON_FAIL_RETRY,
                    max_async_requests=env['max-async-requests']) as api:
        if env['mode'] == MODE_CLEAR_CACHE:
            api.clean_all()
        elif env['mode'] == MODE_CLEAR_IMAGES:
            api.clean_images()
        elif env['mode'] == MODE_CACHE_URLS:
            classes = api.get_wnid_info(env['classes'], env['recursive'], env['deep'])
            wnids = [c[0] for c in classes]
            api.cache_urls(wnids)
        elif env['mode'] == MODE_LOAD_PICTURES:
            print('load')
            classes = api.get_wnid_info(env['classes'], env['recursive'], env['deep'])
            wnids = [c[0] for c in classes]
            urls = api.get_urls(wnids)
            api.fetch(urls)

    print('\nDone')


if __name__ == '__main__':
    main()
