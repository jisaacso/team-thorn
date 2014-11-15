import re
import urllib2
def isEntity(mystr):
    entity_types = ['PERSON', 'LOCATION', 'ORGANIZATION', 'PERCENT', 'DATE', 'TIME',\
                    'MONEY']
    for entity_type in entity_types:
        if entity_type in mystr:
            return True
    return False
            
def tagtext(txt, host, port):
    taggedText = urllib2.urlopen(urllib2.Request('http://%(host)s:%(port)s/%(query)s' %
                                 dict(host=host, port=port, query=txt)))\
                        .read().lstrip('GET').lstrip()\
                        .rstrip('HTTP/1.1').rstrip()
    if taggedText[0] == '/':
        taggedText = taggedText[1:]
        return taggedText

def joinEntities(taggedText):
    NERTags = ['PERSON', 'LOCATION', 'ORGANIZATION', 'PERCENT', 'DATE', 'TIME',\
                       'MONEY']
    for NERTag in NERTags:
        ORG = re.compile(r'<%(tag)s>(.+?)</%(tag)s>' % dict(tag=NERTag))
        for org in ORG.findall(taggedText):
            taggedText = taggedText.replace('<%(tag)s>%(org)s</%(tag)s>' % dict(tag=NERTag, org=org), '_'.join(org.split(' ')) + '/%(tag)s' % dict(tag=NERTag))

    return taggedText

def main():
    txt = tagtext('Hi my name is Joe Isaacson. I live in San Francisco.', 'localhost', 1234)

    taggedText = joinEntities(txt)
    print taggedText
    
if __name__ == '__main__':
    main()
