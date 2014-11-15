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
        for linenum, line in enumerate(gzip.open(self.fname)):
            # Start with training a model on predicate, "description", only
            if not 'common.topic.description' in line:
                continue

            rdf = line.split('common.topic.description>')
            if not len(rdf) == 2:
                print 'Warning: Errand right carrot, %s' % line
                continue
            # pick out english phrases
            if not '@en' in rdf[1]:
                continue
            value = rdf[1].split('@en')[0]
            #if len(value) <= 1:
            #    continue
            #elif len(value) > 2:
            #    print 'Warning: @ found in value, %s' % value
            
            description = unicode(value.strip().lower().replace('\n',' ').replace('-',''), 'utf-8')
            description = ''.join([c for c in fold_accents(description) if
                                   c.isalpha() or c.isspace()])
            onegrams = description.split()
            #twograms = list()
            #for i in range(0, len(onegrams) - 1):
            #    twograms.append(' '.join( (onegrams[i], onegrams[i+1]) ))
            #yield onegrams + twograms
            yield onegrams

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
