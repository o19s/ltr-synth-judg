from elasticsearch import Elasticsearch
from movieDoc import byTitlePhrase

def moviePopularity(movie):
    if 'vote_count' not in movie:
        return 1.0
    if 'vote_average' not in movie:
        return 1.0

    maxPop = 1.0
    voteCnt = movie['vote_count']
    if voteCnt < 20:
        maxPop = 3.0
    elif voteCnt < 90:
        maxPop = 5.0
    elif voteCnt < 200:
        maxPop = 7.0
    else:
        maxPop = 10.0

    voteAvg = movie['vote_average']
    popularity = maxPop * (voteAvg / 10.0)
    return popularity


def moviePopRange(hits):
    maxPop = 0; minPop = 11
    maxPopDoc = minPopDoc = None
    for doc in hits:
        pop = moviePopularity(doc['_source'])
        if pop > maxPop:
            maxPop = pop
            maxPopDoc = doc['_source']
        if pop < minPop:
            minPop = pop
            minPopDoc = doc['_source']
    if minPop == maxPop:
        maxPop += 0.001

    return minPop, maxPop



if __name__ == "__main__":
    es = Elasticsearch()
    from sys import argv
    for movie in byTitlePhrase(titleSearch=argv[1], es=es):
        pop = moviePopularity(movie)
        print(movie['title'])
        print(movie['id'])
        print(movie['overview'])
        print("avg:%s | cnt:%s => %s" % (movie['vote_average'], movie['vote_count'], pop))
