import json
import pandas as pd

def jsonifyItAll(df2):
    df2['postCreationDate'] = df2['postCreationDate'].apply(lambda line: line[:7])
    g = df2.groupby(['postCreationDate']) 
    d = dict()
    for k, v in g:
        d[k] = dict()
        counts = v.groupby(['City', 'OK']).count()['postCreationDate']
        for city, state in counts.keys():
            if not state in d[k]:
                d[k][state] = dict()
                d[k][state]['count'] = 0
            cts = counts[(city, state)]
            d[k][state][city] = cts
            d[k][state]['count'] += cts
    

if __name__ == '__main__':

    stateCounter == pd.read_csv('../data/escort_all/escort_all_states.tsv', delimiter='\t')
    
    with open('../data/counts.json', 'w') as fout:
        fout.writes(json.dumps(d)) 