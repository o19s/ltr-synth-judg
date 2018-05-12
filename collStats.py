import os
import atexit
import json
from popularity import moviePopRange
from elasticsearch import Elasticsearch
from movieDoc import byCollPhrase

def collectionLookup(es, collId, docId, index='tmdb'):
    collQ = {
        "size": 50,
        "sort": [
            {"vote_average": "desc"}
        ],
        "query": {
            "bool": {
                "must": [
                    {"match": {
                        "belongs_to_collection.id": collId}}
                ]
            }
        }
    }

    resp = es.search(index=index, body=collQ)
    collectionLookup.cache[collId] = resp

    minPop, maxPop = moviePopRange(resp['hits']['hits'])
    delIdx = False
    for idx, hit in enumerate(resp['hits']['hits']):
        if str(hit['_id']) == str(docId):
            delIdx = idx
    if delIdx:
        del resp['hits']['hits'][delIdx]
    return resp, minPop, maxPop

collectionLookup.cache = {}
if os.path.exists('coll_cache.json'):
    with open('coll_cache.json') as f:
        collectionLookup.cache = json.load(f)


@atexit.register
def dumpCache():
    with open('coll_cache.json', 'w') as f:
        print('writing coll cache')
        json.dump(collectionLookup.cache, f)

if __name__ == "__main__":
    es = Elasticsearch()
    from sys import argv
    for movie in byCollPhrase(collNameSearch=argv[1], es=es):
        print(movie['belongs_to_collection']['name'])
        collId=movie['belongs_to_collection']['id']
        resp, minPop, maxPop = collectionLookup(es=es,
                                          collId=collId,
                                          docId=None)
        print("minPop %s maxPop %s" % (minPop, maxPop))
