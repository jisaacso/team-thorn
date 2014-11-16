import json
import pandas as pd
import numpy as np

def meanApplier(line):
    inl = []
    for i in line:
        inl += [float(j.strip().lstrip("'").rstrip("'")) for j in i.rstrip(']').lstrip('[').split(',') if j]
    return np.mean(inl)

def jsonifyItAll(df2):
    df2['postCreationDate'] = df2['postCreationDate'].apply(lambda line: line[:7])
    g = df2.groupby(['postCreationDate']) 
    d = dict()
    for k, v in g:
        d[k] = dict()
        counts = v.groupby(['City', 'State']).count()['postCreationDate']
        for city, state in counts.keys():
            if not state in d[k]:
                d[k][state] = dict()
                d[k][state]['counts'] = 0
                d[k][state]['prices'] = 0
            cts = counts[(city, state)]
            d[k][state][city] = dict()
            d[k][state][city]['counts'] = cts
            d[k][state]['counts'] += cts
        prices = v.groupby(['City', 'State'])['Dolla']\
                  .apply(lambda line: meanApplier(line))
        for city, state in prices.keys():
            pcs = prices[(city, state)]
            d[k][state][city]['prices'] = pcs
            d[k][state]['prices'] += pcs
        
    for date in d:
        for state in d[date]:
            d[date][state]['prices'] /= float(len(d[date][state]) - 2)
    return d

if __name__ == '__main__':

    df2 = pd.read_csv('../data/escort_all/escort_all_states_money.tsv', \
                      delimiter='\t', header=None, names=['postCreationDate',\
                                                      'City', 'State', 'Dolla'])
    d = jsonifyItAll(df2)
    
    with open('../data/counts_prices.json', 'w') as fout:
        fout.write(json.dumps(d)) 