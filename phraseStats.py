import os
import atexit
import json
from popularity import moviePopRange
from elasticsearch import Elasticsearch

def phraseStats(phrase, es, index='tmdb'):
    termsQ = {
        "size": 5000,
        "sort": [
            {"vote_average": "desc"}
        ],
        "query": {
            "match_phrase": {
                "text_all.en": phrase
            }
        }
    }
    resp = es.search(index=index, body=termsQ)

    phraseFreq = resp['hits']['total'];

    minPop, maxPop = moviePopRange(resp['hits']['hits'])
    return phraseFreq, minPop, maxPop



def phraseDocFreq(text, es):
    lookupText = text.lower()
    if True: # lookupText not in phraseDocFreq.cache:
        pf, minPop, maxPop = phraseStats(phrase=text, es=es)
        phraseDocFreq.cache[lookupText] = [pf, minPop, maxPop]
        return pf, minPop, maxPop
    else:
        return phraseDocFreq.cache[lookupText][0], \
               phraseDocFreq.cache[lookupText][1], \
               phraseDocFreq.cache[lookupText][2]
phraseDocFreq.cache={}
if os.path.exists('df_cache.json'):
    with open('df_cache.json') as f:
        phraseDocFreq.cache = json.load(f)

@atexit.register
def saveCache():
    with open('df_cache.json', 'w') as f:
        print('writing pf cache')
        json.dump(phraseDocFreq.cache, f)

if __name__ == "__main__":
    es = Elasticsearch()
    from sys import argv
    pf, minPop, maxPop = phraseStats(es=es, phrase=argv[1])
    print("%s => freq %s minPop %s maxPop %s" % (argv[1], pf, minPop, maxPop))
