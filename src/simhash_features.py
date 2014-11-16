"""
Taken from:
https://github.com/liangsun/simhash/blob/master/simhash/__init__.py

Renamed Simhash to Bithash to avoid clash with Simhash-py and Simhash-db
"""

#Created by Liang Sun in 2013
import re
import logging
import collections


class Bithash(object):
    def __init__(self, value, f=64, reg=ur'[\w\u4e00-\u9fff]+', hashfunc=None):
        '''
        `f` is the dimensions of fingerprints

        `reg` is meaningful only when `value` is basestring and describes
        what is considered to be a letter inside parsed string. Regexp
        object can also be specified (some attempt to handle any letters
        is to specify reg=re.compile(r'\w', re.UNICODE))

        `hashfunc` accepts a utf-8 encoded string and returns a unsigned
        integer in at least `f` bits.
        '''

        self.f = f
        self.reg = reg
        self.value = None

        if hashfunc is None:
            import hashlib
            self.hashfunc = lambda x: int(hashlib.md5(x).hexdigest(), 16)
        else:
            self.hashfunc = hashfunc

        if isinstance(value, Bithash):
            self.value = value.value
        elif isinstance(value, basestring):
            #self.build_by_text(unicode(value))
            self.build_by_text(value)
        elif isinstance(value, collections.Iterable):
            self.build_by_features(value)
        elif isinstance(value, long):
            self.value = value
        else:
            raise Exception('Bad parameter')

    def _slide(self, content, width=3):
        return [content[i:i+width] for i in xrange(max(len(content)-width+1, 1))]

    def _tokenize(self, content):
        ans = []
        content = content.lower()
        #content = ' '.join(re.findall(self.reg, content))
        ans = self._slide(content)
        #ans = content.split()
        return ans

    def build_by_text(self, content):
        features = self._tokenize(content)
        self._features = features
        return self.build_by_features(features)

    def build_by_features(self, features):
        #hashs = [self.hashfunc(w.encode('utf-8')) for w in features]
        hashs = [self.hashfunc(w) for w in features]
        
        v = [0]*self.f
        masks = [1 << i for i in xrange(self.f)]
        for h in hashs:
            for i in xrange(self.f):
                v[i] += 1 if h & masks[i] else -1
        ans = 0
        for i in xrange(self.f):
            if v[i] >= 0:
                ans |= masks[i]
        self.value = ans

    def distance(self, another):
        x = (self.value ^ another.value) & ((1 << self.f) - 1)
        ans = 0
        while x:
            ans += 1
            x &= x-1
        return ans

class BithashIndex(object):
    """
    bithash is an instance of Bithash
    return a list of obj_id, which is in type of str
    """

    def get_near_dups(self, bithash, tolerance=2):
        ans = set()

        for offset in [0, 21, 42]:
            n = bithash.value >> offset & (42==offset and 0x3fffff or 0x1fffff)
            key = '%x:%x' % (n, offset/21)
            ret = self.bucket.get(key, set())
            logging.debug('key:%s', key)
            if len(ret) > 100:
                logging.warning('Big bucket found. key:%s, len(ret):%s', key, len(ret))

            for r in ret:
                sim2, obj_id = r.split(',', 1)
                sim2 = Bithash(long(sim2, 16))

                d = bithash.distance(sim2)
                if d <= tolerance:
                    ans.add(obj_id)
        return list(ans)

    # obj_id is a string
    # bithash is an instance of bithash
    def add(self, obj_id, bithash):
        for offset in [0, 21, 42]:
            c = bithash.value >> offset & (42==offset and 0x3fffff or 0x1fffff)

            k = '%x:%x' % (c, offset/21)
            v = '%x,%s' % (bithash.value, obj_id)

            self.bucket.setdefault(k, set())
            self.bucket[k].add(v)

    # objs is a list of (obj_id, bithash)
    # obj_id is a string, bithash is an instance of Bithash
    def __init__(self, objs):
        count = len(objs)
        logging.info('Initializing %s data.', count)

        self.bucket = {}

        for i, q in enumerate(objs):
            if i % 10000 == 0 or i == count-1:
                logging.info('%s/%s', i+1, count)

            self.add(*q)

    def bucket_size(self):
        return len(self.bucket)