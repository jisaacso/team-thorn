import json
import pandas as pd
import numpy as np
from getStates import getState

def jsonifyItAllNow(G, cityStateMapper, phoneToCity):
    outDict = dict()
    for k in G.keys():
        v = G[k]
        k = int(k)
        outDict[k] = dict()
        for k2 in v: #.keys()
            k2 = int(k2)
            cities = phoneToCity[k2]
            for city in cities:
                state = cityStateMapper[city]
                if not state in outDict[k]:
                    outDict[k][state] = dict()
                    outDict[k][state]['counts'] = 0
                if not city in outDict[k][state]:
                    outDict[k][state][city] = dict()
                    outDict[k][state][city]['counts'] = 0
                outDict[k][state]['counts'] += 1
                outDict[k][state][city]['counts'] += 1
    return outDict

def getAllTheStates(es, phoneToCity):
    cityStateMapper = dict()
    for phone in phoneToCity.keys():
        for city in phoneToCity[phone]:
            if city in cityStateMapper:
                continue
            cityStateMapper[city] = getState(es, city)
    return cityStateMapper

def getCities(df, phoneNos):

    phoneToCity = dict()
    for phoneNo in phoneNos:
        phoneToCity[phoneNo] = list(df['City'][np.where(df['postPhone'] == int(phoneNo))[0]].dropna().unique())

def jsonReader(G):
    V = set()
    for k in G.values():
        for k2 in k:
            V.add(k2)
    uniqs = set(G.keys()).union(V)
    return [int(i) for i in uniqs]        
            

if __name__ == '__main__':

    with open('../data/pimp-graph.json', 'r') as fin:
        g = fin.read()
    G = json.loads(g)
    phoneNos = jsonReader(G)

    df = pd.read_csv('../data/escort_all/escort_all.tsv', delimiter='\t')
    df = df.drop(np.where(np.isnan(df['postPhone']))[0], axis=0)
    df['postPhone'] = df['postPhone'].astype(int)