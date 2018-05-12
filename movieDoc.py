
def byTitlePhrase(es, titleSearch="star trek", index='tmdb'):
    allQ = {
        "size": 1,
        "query": {
            "match_phrase": {"title": titleSearch}
        }
    }
    resp = es.search(index=index, body=allQ)
    for doc in resp['hits']['hits']:
        yield doc['_source']


def byCollPhrase(es, collNameSearch="star trek", index='tmdb'):
    allQ = {
        "size": 1,
        "query": {
            "match_phrase": {"belongs_to_collection.name": collNameSearch}
        }
    }
    resp = es.search(index=index, body=allQ)
    for doc in resp['hits']['hits']:
        yield doc['_source']
