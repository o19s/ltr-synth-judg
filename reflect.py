from reflector import Reflector
from elasticsearch import Elasticsearch



def docs(es, titleSearch="star trek", index='tmdb'):
    allQ = {
        "size": 1000,
        "query": {
            "match_phrase": {"text_all.en": titleSearch}
        }
    }
    resp = es.search(index=index, body=allQ)
    for doc in resp['hits']['hits']:
        yield doc['_source']



if __name__ == "__main__":
    es = Elasticsearch()
    from sys import argv
    for doc in docs(titleSearch=argv[1], es=es):
        print(doc['title'])
        print(doc['overview'])
        rfor = Reflector(doc, es=es)
        for queryScore in rfor.queries():
            if queryScore[1] > 499:
                print(" --- %s %s " % queryScore)






