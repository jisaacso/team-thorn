import sys
import json
import numpy as np
from Queue import Queue

def inPeterList(no):
    peterList = [
        "2098184104",
        "6025188869",
        "3232178830",
        "3233087401",
        "4152403388",
        "4242074892",
        "5102500745",
        "5106814957",
        "5108754610",
        "6264372914",
        "6614184763",
        "6618023923",
        "7146401124",
        "7148727341",
        "7603087412",
        "8054677213",
        "9162121024",
        "9162560679",
        "9166924482",
        "9169057879",
        "9169149610",
        "9256954308"
    ]

    return no in peterList

def buildNetwork(es, nloops, accuracy=.8):
    seedPhoneList = ['6025188869']
    q = Queue()
    for entry in seedPhoneList:
        q.put(entry)
    queueHash = set()
    [queueHash.add(phone) for phone in seedPhoneList]    
    #for infidx, seedPhone in enumerate(seedPhoneList):
    infidx = -1
    ret = set()
    while not q.empty():
        infidx += 1
        if infidx > nloops:
            break
        seedPhone = q.get()
        ret.add(seedPhone)
        hits = getDocFromPhone(es, seedPhone)
        allThePhones = set()
        for hit in hits:
            postText = hit['_source']['post_text']
            similarPosts = getSimilarText(es, postText)

            scoreDist = [similarPost['_score'] for similarPost in similarPosts]
            mu = np.mean(scoreDist)
            sigma = np.std(scoreDist)
            normalizedScores = abs(scoreDist - mu) / float(sigma)

            for idx, similarPost in enumerate(similarPosts):
                #print '='*40
                #print similarPost['_source']['post_text']
                phoneNo = similarPost['_source']['post_phone']
                #print similarPost['_score']
                #print normalizedScores[idx]
                if normalizedScores[idx] <= accuracy:                  
                    allThePhones.add(phoneNo)

        newPhones = list(allThePhones - queueHash)
        #print newPhones            
        [q.put(i) for i in newPhones]
        [ret.add(i) for i in newPhones]
         
    print '\n'.join(['%s, %s' %(i, inPeterList(i)) for i in ret])
    print infidx    
        

def getDocFromPhone(es, phone):
    q = """{
    "query": {
     "filtered": {
      "filter": {
       "term": {"post_phone": "%(phone)s"}
      }
     }
    }
    }
    """
    qry = q % dict(phone=phone)
    res = es.search(index='thorn', size=10, body=json.loads(qry))

    response = res['hits']['hits']

    if not len(response) > 0:
        print 'no phone returned for %s' % phone
        return ''
        
    return response

    
def getSimilarText(es, text):
    q = """{
       "query": {
          "bool": {
             "must": {
                "match": {
                   "post_text": "%(text)s"
                }
             }
          }
       }
    }
    """
    qry = q % dict(text=text)
    res = es.search(index='thorn', size=10, body=json.loads(qry))

    response = res['hits']['hits']

    if not len(response) > 0:
        print "no response returned for query " 
        return ''

    return response


if __name__ == '__main__':
    q = str(sys.args[1])

    getSimilarText(es, q)