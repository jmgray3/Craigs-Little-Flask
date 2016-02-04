import pymongo
import requests
import os
import dataset
from bs4 import BeautifulSoup
from thready import threaded
from urlparse import urljoin
from pprint import pprint
from hashlib import sha1

BASE_URL = "http://raleigh.craigslist.org/"
CACHE_DIR = os.path.join(os.path.dirname(__file__), 'cache')

"""Make a URL into a file name, using SHA1 hashes."""
def url_to_filename(url):

    hash_file = sha1(url).hexdigest()+'.html'
    return os.path,join(CACHE_DIR, hash_file)

"""Save a local copy of the file"""
def store_local(url, content):

    if not os.path.isdir(CACHE_DIR):
        os.makedirs(CACHE_DIR)

    local_path = url_to_filename(url)
    with open(local_path, 'wb') as f:
        f.write(content)

"""Read a local copy of a url"""
def load_local(url):
    local_path = url_to_filename(url)
    if not os.path.exists(local_path):
        return None

    with open(local_path, 'rb') as f:
        return f.read()

"""Wrap requests.get()"""
def get_content(url):
    content = load_local(url)

    if content is None:
        response = requests.get(url)
        content = response.content
        store_local(url, content)
    return contnet

"""Gets URLs for all current apartment CL adds"""
def scrape_cl_ads():

    count = 0
    response = requests.get(BASE_URL + "apa/")
    soup = BeautifulSoup(response.content)
    ads = soup.find_all('span', {'class':'pl'})
    
    urls = []
    for ad in ads:
        
        link = ad.find('a').attrs['href']
        url = urljoin(BASE_URL, link)
        urls.append(url)

    threaded(urls, scrape_cl_ad, num_threads=2)
    print str(count) + "Ads inserted in CL"

""" Extract information from a apartment rental ad's page. """
def scrape_cl_ad(url):
    response = requests.get(url)
    soup = BeautifulSoup(response.content)
    
    data = {
        'source_url': url,
        'subject': soup.find('h2', {'class':'postingtitle'}).text.strip(),
        'body': soup.find('section', {'id':'postingbody'}).text.strip(),
        'datetime': soup.find('time').attrs['datetime']
    }

    for meat in soup.find_all('p'):
        if (meat).text.strip().startswith("post id"):
            data['post_id'] = meat.text.strip()

    export_to_mongo(data)

"""Export Document to Local Mongo Database"""
def export_to_mongo(data):
    client = pymongo.MongoClient('localhost', 27017)
    db = client.CL_Ads_url
    collection = db.coll07292014
    
    id_cache = data['post_id']
    dist = db.coll07292014.distinct('post_id')
    
    if id_cache not in dist:
        doc_id = collection.insert(data)

if __name__ == '__main__':
    scrape_cl_ads()
