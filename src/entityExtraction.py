#from gensim import models
import argparse
import base64
import gzip
import unicodedata
import logging
import os
import sys
from tagger import tagtext, joinEntities, isEntity
import re

logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s',
                    level=logging.INFO)

def fold_accents(raw):
    if type(raw) == str:
        raw = unicode(raw, 'utf-8')
    return ''.join([c for c in unicodedata.normalize('NFKD', raw).encode('ascii', 'ignore')])
                    #if not unicodedata.combining(c)))
def isspecialchar(char):
    specialchars = ['$']
    return char in specialchars

def strcleaner(postText):
    wspaceNuker = re.compile(' +')

    postText = ''.join([c for c in fold_accents(postText)])
    postText = ''.join([c for c in postText if c.isspace() or c.isalnum() or isspecialchar(c)])
    postText = ''.join([c for c in fold_accents(postText)]).lower()
    postText = postText.replace('microsoftsqlserverdtspipelineblobcolumn', '')
    postText = re.sub(wspaceNuker, ' ', postText)
    return postText

def jsonifyEntities(entities, entityDict):
    for entity in entities.split(','):
        val = entity.split('/')
        txt = val[:-1]
        ent = val[-1]
        try:
            entityDict[ent.rstrip('$')] += txt
        except KeyError:
            print 'keyError %s' % ent
    return entityDict
        
        
def tagPostText(postText):
    #dirty hack. 4% of the descriptions are nothing but a web link.
    edict = dict()
    etypes = ['PERSON', 'LOCATION', 'ORGANIZATION', 'PERCENT', 'DATE', 'TIME',\
                    'MONEY']
    for k in etypes:
        edict[k] = list()

    if postText[:4] == 'http':
        return '', edict

    postText = strcleaner(postText)
    
    taggedText = tagtext(postText, 'localhost', 22222)
    if not taggedText:
        return '', edict

    taggedText = joinEntities(taggedText)

    entities = ','.join([w for w in taggedText.split(' ') if isEntity(w)])
    if entities:
        entities = jsonifyEntities(entities, edict)
    else:
        entities = edict
    
    return taggedText, entities
    
class Entities(object):
    def __init__(self, fname):
        self.fname = fname
        self.entities = list()
    def __iter__(self):
        for linenum, line in enumerate(open(self.fname)):
            entry = line.split('\t')
            if not len(entry) == 13:
                print 'Entry %s was crap' % linenum

            siteName = entry[0]
            city = entry[1]
            location = entry[2]
            postName = entry[3]
            postText = entry[5]
            postDate = entry[6]

            taggedText, ents = tagPostText(postText)

            yield ents

"""
def w2v(fname, modelSize, sampleThresh, negThresh, outFileName):
    iterobj = RDFTriples(fname)
    os.system("taskset -p 0xff %d" % os.getpid())
    model = models.Word2Vec(iterobj, size=modelSize, sample=sampleThresh,
                            negative=negThresh, workers=8)

    outName = '/home/jisaacso/projects/topic-modeling/trained_models/'+\
        'freebase-w2v-%s-%s-%s-%s.bin' % (modelSize, sampleThresh, negThresh, outFileName)
    #model.save('./trained_models/freebase-w2v-1000.bin')
    model.save(outName)

    return model
"""
"""
if __name__ == '__main__':
    modelSize = int(sys.argv[1])
    sampleThresh = float(sys.argv[2])
    negThresh = float(sys.argv[3])
    if len(sys.argv) == 5:
        outFileName = str(sys.argv[4])
    else:
        outFileName = ''

    model = w2v('/datasets/freebase/freebase-rdf-2014-07-13-00-00.gz', 
                modelSize, sampleThresh, negThresh, outFileName)
"""
if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('--text', type=str)
    args = parser.parse_args()
    if not args.text:
        print 'Usage: >> python entityExtractor.py --text "<base64 encoded text to tag>"'
    print tagPostText(base64.b64decode(args.text))

    