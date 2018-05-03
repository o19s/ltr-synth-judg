from reflector import Reflector
from elasticsearch.helpers import scan
from elasticsearch import Elasticsearch
from itertools import islice

def seriesMovie(movie):
    return 'title' in movie and 'overview' in movie and 'belongs_to_collection' in movie and movie['belongs_to_collection'] is not None

def reflectSeries(es, index='tmdb', doc_type='movie'):
    """ Series provide the best reflections, so we'll limit our scope
        to those. After all this is training data!"""
    reflections = {}
    for hit in islice(scan(es, index=index, doc_type=doc_type, query={"query": {"match_all": {}}}),5000):
        movie = hit['_source']
        docId = hit['_id']
        # Movies part of a series generate the best training data
        if seriesMovie(movie):
            title = movie['title']
            print("-- %s --" % title)
            reflections[title] = Reflector(es=es, docTitle=title, docId=docId, doc=movie, index='tmdb')
    return reflections


def invertReflections(reflections):
    """ Take synthetic per-query keywords and turn them into
        dictionary oriented"""
    qcsByKeyword = {}
    for title, ref in reflections.items():
        for phrase, qc in ref.textQueryCandidates.items():
            print("Adding %s" % qc)
            if phrase in qcsByKeyword:
                qcsByKeyword[phrase].append(qc)
            else:
                qcsByKeyword[phrase] = [qc]

    for phrase, qcs in qcsByKeyword.items():
        qcs.sort(key=lambda qc: qc.asJudgment(), reverse=True)
    return qcsByKeyword

def insertNegativeJudgments(reflections):
    allNp = set()
    for title, ref in reflections.items():
        for phrase, qc in ref.textQueryCandidates.items():
            allNp.add(qc.qp)

    # Apply each qc to every other qc, add as a negative judgment if not mentioned
    for title, ref in reflections.items():
        for np in allNp:
            print("ADD NEG JUDG %s" % np)
            ref.addNegativeJudgment(np)


if __name__ == "__main__":
    es=Elasticsearch()
    reflections = reflectSeries(es)
    insertNegativeJudgments(reflections=reflections)
    inverted = invertReflections(reflections)
    for phrase, qcs in inverted.items():
        if len(qcs) > 2 and qcs[0].asJudgment() >= 3:
            print(" -- %s -- " % phrase)
            for qc in qcs:
                print(qc)

