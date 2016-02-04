import os
from hashlib import sha1

CACHE_DIR = os.path.join(os.path.dirname(__file__), 'cache')

def url_to_filename(url)