import json
from elasticsearch import Elasticsearch
from entityExtraction import tagPostText
import re

def getCost(mystr, regex):
    vals = []
    for rg in regex:
        matches = re.findall(rg, mystr)
        if matches:
            vals += matches
    return vals
    
def getState(es, city):
    q = """{
     "query": {
      "function_score": {
       "query": {
       "filtered": {
        "query": {
         "match": {"gn_name": "%(city)s"}
        },
        "filter": {
          "term": {"gn_country": "us"}
        }
       }},
       "functions": [ {
         "script_score" : {
          "script": "doc['gn_pop'].value > 0 ? .8 * _score + .2 * log(doc['gn_pop'].value) / 8 : _score * 0.00000000001;"
         }
        } ],
       "boost_mode": "replace"
      }
     }
    }
    """
    qry = q % dict(city=city)
    res = es.search(index='gazetteer', size=1, body=json.loads(qry))

    response = res['hits']['hits']

    if not len(response) > 0:
        print "no response returned for %s " % city
        return ''

    try:
        city = response[0]['_source']['gn_admin1']
    except KeyError, IndexError:
        print 'no city given for %s' % response
        return ''
    
    return city

if __name__ == '__main__':

    regex1 = re.compile('\$\s*([4-9][0-9])[^0-9]')
    regex2 = re.compile('\$\s*([1-9][0-9]0)[^0-9]')
    regex3 = re.compile('[^0-9]([4-9][0-9])\s*\$')
    regex4 = re.compile('[^0-9]([1-9][0-9]0)\s*\$')
    regex = (regex1, regex2, regex3, regex4)
    
    es = Elasticsearch(host='localhost', port=9200)
    with open('../data/escort_all/escort_all.tsv', 'r') as fin:
        with open('../data/escort_all/escort_all_states_money.tsv', 'wb') as fout:
            for idx, line in enumerate(fin):
                tokens = line.split('\t')
                siteName = tokens[0]
                city = tokens[1]
                location = tokens[2]
                postName = tokens[3]
                postText = tokens[5]
                date = tokens[6]
                state = getState(es, city)
                
                
                cost = getCost(postName + '. ' + postText, regex)
                #txt, tags = tagPostText(postName + '. ' + postText)
                #cost = tags['MONEY']
                #if not cost:
                #    cost = ''
                
                fout.write(date + '\t' + city + '\t' + state + '\t' + str(cost) + '\n')

                if idx % 10000 == 0:
                    print idx / 7000000.0