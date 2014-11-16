import json
from elasticsearch import Elasticsearch

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
    es = Elasticsearch(host='localhost', port=9200)
    """
    entgen = Entities('../data/escort_all/escort_all.tsv')
    
    with open('../data/escort_all/escort_all_money.tsv', 'wb') as fout:
        for idx, line in enumerate(entgen):
            if line['MONEY']:
                print line
            
            #fout.write(date + '\t' + city + '\t' + state + '\n')
            
            if idx % 10000 == 0:
                print idx / 7000000.0

    """
    with open('../data/escort_all/escort_all.tsv', 'r') as fin:
        with open('../data/escort_all/escort_all_states.tsv', 'wb') as fout:
            for idx, line in enumerate(fin):
                tokens = line.split('\t')
                city = tokens[1]
                date = tokens[6]
                state = getState(es, city)

                fout.write(date + '\t' + city + '\t' + state + '\n')

                if idx % 10000 == 0:
                    print idx / 7000000.0