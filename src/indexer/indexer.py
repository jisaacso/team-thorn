#!/usr/bin/python

import os.path
import tornado.ioloop
from tornadoes import ESConnection

from greplin import scales
from greplin.scales.meter import MeterStat

import json
from hashlib import md5

import re
import unicodedata

STATS = scales.collection('/index', MeterStat('docs'))

BASE_PATH = '/Users/jisaacso/Documents/projects/bayes-impact/team-thorn/data/escort_all'
FBDUMP = os.path.join(BASE_PATH, 'escort_all.tsv')

es = ESConnection('localhost', 9200)
es.httprequest_kwargs = {
    'request_timeout': 1500.00,
    'connect_timeout': 1500.00
}
wspaceNuker = re.compile(' +')
def fold_accents(raw):
    if type(raw) == str:
        raw = unicode(raw, 'utf-8')
    return ''.join([c for c in unicodedata.normalize('NFKD', raw).encode('ascii', 'ignore')])

def isspecialchar(char):
    specialchars = ['$', '.']
    return char in specialchars

def fb_to_es(line):
    entry = line.split('\t')
    if not len(entry) == 13:
        return None
        
    siteName = entry[0]
    city = entry[1]
    location = entry[2]
    postName = entry[3]
    postText = entry[5]
    postDate = entry[6]
    postEmail = entry[9]
    postPhone = entry[10]

    postText = ''.join([c for c in fold_accents(postText)])
    postText = ''.join([c for c in postText if c.isspace() or c.isalnum() or isspecialchar(c)])
    postText = postText.lower()
    postText = postText.replace('out.microsoft.sqlserver.dts.pipeline.blobcolumn', '')
    postText = postText.replace('one.microsoft.sqlserver.dts.pipeline.blobcolumn', '')
    postText = postText.replace('microsoft.sqlserver.dts.pipeline.blobcolumn', '')
    postText = re.sub(wspaceNuker, ' ', postText)

    if postText[:4] == 'http':
        return None
    
    postDate = postDate.replace('+00:00', '')
    
    
    return dict(city=city, location=location, post_name=postName,
                post_text=postText, post_date=postDate, post_email=postEmail,
                post_phone=postPhone)
    
def fb_docs():
    with open(FBDUMP, 'r') as fin:
        for line in fin:
            yield fb_to_es(line)
        
docs = fb_docs()

def index(response):
    try:
        STATS.docs.mark()

        if STATS.docs['count'] % 1000 == 0:
            print 'Rate: ' + str(STATS.docs['m1'])

        doc = next(docs)
        es.put('thorn', 'posting',
            md5(json.dumps(doc)).hexdigest(), doc, callback=index)
    except StopIteration:
        print 'ahh'
        pass

if __name__ == '__main__':
    for i in xrange(10):
        index(None)

    tornado.ioloop.IOLoop.instance().start()
