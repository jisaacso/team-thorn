#from gensim import models
import gzip
import unicodedata
import logging
import os
import sys

logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s',
                    level=logging.INFO)

def fold_accents(raw):
    if type(raw) == str:
        raw = unicode(raw, 'utf-8')
    return ''.join([c for c in unicodedata.normalize('NFKD', raw).encode('ascii', 'ignore')])
                    #if not unicodedata.combining(c)))

class RDFTriples(object):
    def __init__(self, fname):
        self.fname = fname

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

            #dirty hack. 4% of the descriptions are nothing but a web link.
            if postText[:4] == 'http':
                continue

            postText = ''.join([c for c in fold_accents(postText)])
            postText = ''.join([c for c in postText if c.isspace() or c.isalnum()])
            postText = ''.join([c for c in fold_accents(postText)]).lower()
            postText.replace('microsoftsqlserverdtspipelineblobcolumn', '')
            yield postText


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
