import json
from entityExtraction import Entities
if __name__ == '__main__':

    entgen = Entities('../data/escort_all/escort_all.tsv')
    
    with open('../data/escort_all/escort_all_money.tsv', 'wb') as fout:
        for idx, line in enumerate(entgen):
            if line['MONEY']:
                print line
            
            #fout.write(date + '\t' + city + '\t' + state + '\n')
            
            if idx % 10000 == 0:
                print idx / 7000000.0