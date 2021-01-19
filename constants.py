IMGNETAPI_CHILDS = 'http://www.image-net.org/api/text/wordnet.structure.hyponym?wnid={0}'
IMGNETAPI_ALL_CHILDS = IMGNETAPI_CHILDS + '&full=1'
IMGNETAPI_URLS = 'http://www.image-net.org/api/text/imagenet.synset.geturls?wnid={0}'
IMGNETAPI_SYNSETWORDS = 'http://www.image-net.org/api/text/wordnet.synset.getwords?wnid={0}'
IMGNETAPI_ALLHIERARCHY = 'http://www.image-net.org/archive/wordnet.is_a.txt'
IMGNETAPI_ALLWORDS = 'http://www.image-net.org/archive/words.txt'
IMGNETAPI_RELEASESTATUS = 'http://www.image-net.org/api/xml/ReleaseStatus.xml'

MODE_CLEAR_CACHE = 0
MODE_LOAD_PICTURES = 1
MODE_SAVE_CLASSES = 2
MODE_CACHE_URLS = 4

MAX_RECONNECT_ATTEMPTS = 3
MAX_ASYNC_REQUESTS = 150
MAX_ASYNC_IMAGENET_REQUESTS = 50

PG_INSERT_BATCH_SIZE = 2000
