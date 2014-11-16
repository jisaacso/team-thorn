from pybloomfilter import BloomFilter
import md5

def dedup(fname):
    bf = BloomFilter(1E8, 0.01)
    
    with open(fname, 'r') as fin:
        with open('deduped.tsv', 'w') as fout:
            for line in fin:
                splitLine = line.split('\t')
                description = splitLine[5]
                if bf.add(md5.new(description).digest()):
                    continue
                else:
                    fout.write(line)

if __name__ == '__main__':
    dedup('../data/escort_all/escort_all.tsv')
            