import sys
import getopt
import imagenet_pkg.images_puller as images_puller
import re
import imagenet_pkg.class_distributer as cd
from imagenet_pkg.constants import *
import psycopg2 as pspg
import psycopg2.extensions

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
    env['db_user'] = 'postgres'
    env['db_name'] = 'img-net'

    if len(sys.argv) > 1:
        try:
            args = getopt.getopt(sys.argv[1:], 'hc:RC:p:d:r:v:m:n:', [
                'help',
                'usage',
                'classes=',
                'fetch-ratio='
                'recursive',
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

            if not env['classes'] and env['mode'] != MODE_CLEAR_CACHE:
                print_usage()
            if env['max-classes'] and env['max-classes'] > len(env['classes']):
                print('Error: \'max-classes\' cannot be greater than number of classes specified.')
                print_usage()
        except Exception as ex:
            print_usage()


def print_usage(exit_code=1):
    print('===== USAGE =====\n'
          'imagenet-pull '
          '-c CLASSES '
          '[-r FETCH_RATIO] '
          '[-R] '
          '[-C MAX_CLASSES] '
          # '[-p PICTURES_PER_CLASS] '
          '[-v IMAGENET_RELEASE] '
          '[-n MAX_ASYNC_REQUESTS] '
          '[-m MODE]\n'
          '--classes CLASSES '
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

    env['db_conn'] = pspg.connect("user=postgres dbname=image-net")

    class_manager = cd.ClassDistributer(env)
    env['class_manager'] = class_manager
    image_manager = images_puller.ImagesWorker(env)
    env['image_manager'] = image_manager

    if env['mode'] == MODE_CLEAR_CACHE:
        class_manager.clean()
        image_manager.clean()
        print('Successfully cleared.')
        return

    if env['mode'] == MODE_CLEAR_IMAGES:
        image_manager.clean()
        print('Successfull cleared.')
        return

    class_manager.init_db(True)
    # classes is a list of (wnid, short_name, full_name, path)
    classes = class_manager.get_env_classes()
    classes = sorted(classes, key=lambda x: x[3])
    if env['max-classes'] and len(classes) > env['max-classes']:
        classes = classes[:env['max-classes']]

    if env['mode'] == MODE_CACHE_URLS:
        class_manager.cache_urls([x[0] for x in classes], min_valid_ratio=1, debug=True)
        print('Successfully cached.')

    if env['mode'] == MODE_LOAD_PICTURES:
        # get all urls we need
        # urls is an array of (url_id, wnid, url, state_id)
        print('Getting urls...')
        urls = class_manager.get_urls_of([cls[0] for cls in classes])

        image_manager.fetch(urls, debug=True)
        print('Successfully loaded.')


if __name__ == '__main__':
    main()
