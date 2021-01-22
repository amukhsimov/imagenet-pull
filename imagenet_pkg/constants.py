IMGNETAPI_CHILDS = 'http://www.image-net.org/api/text/wordnet.structure.hyponym?wnid={0}'
IMGNETAPI_ALL_CHILDS = IMGNETAPI_CHILDS + '&full=1'
IMGNETAPI_URLS = 'http://www.image-net.org/api/text/imagenet.synset.geturls?wnid={0}'
IMGNETAPI_SYNSETWORDS = 'http://www.image-net.org/api/text/wordnet.synset.getwords?wnid={0}'
IMGNETAPI_ALLHIERARCHY = 'http://www.image-net.org/archive/wordnet.is_a.txt'
IMGNETAPI_ALLWORDS = 'http://www.image-net.org/archive/words.txt'
IMGNETAPI_RELEASESTATUS = 'http://www.image-net.org/api/xml/ReleaseStatus.xml'

DEFAULT_RELEASE = 'fall2011'

MODE_CLEAR_CACHE = 0
MODE_LOAD_PICTURES = 1
MODE_SAVE_CLASSES = 2
MODE_CACHE_URLS = 4
MODE_CLEAR_IMAGES = 8
MODE_CLEAR_DATABASE = 16

MAX_ASYNC_REQUESTS_DEFAULT = 150
MAX_RECONNECT_ATTEMPTS = 3

URL_ON_FAIL_IGNORE = 0
URL_ON_FAIL_RETRY = 1
URL_SUCCESS = 1
URL_FAILED = 0
