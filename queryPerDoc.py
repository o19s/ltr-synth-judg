from reflector import Reflector
from elasticsearch.helpers import scan
from elasticsearch import Elasticsearch
from itertools import islice
from judgments import Judgment, judgmentsToFile

NUM_MOVIES_TO_SCAN=1000

INCLUDE_NEGATIVE = 1.0
if NUM_MOVIES_TO_SCAN > 5000:
    INCLUDE_NEGATIVE = 0.5

def seriesMovie(movie):
    return 'title' in movie and \
           'overview' in movie and \
            isinstance(movie['overview'], str) and\
            isinstance(movie['title'], str) and\
           'belongs_to_collection' in movie and movie['belongs_to_collection'] is not None

def reflectSeries(es, index='tmdb', doc_type='movie'):
    """ Series provide the best reflections, so we'll limit our scope
        to those. After all this is training data!"""
    reflections = {}
    for hit in islice(scan(es, scroll='30m', index=index, doc_type=doc_type, query={"query": {"match_all": {}}}),NUM_MOVIES_TO_SCAN):
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
        for phrase, qc in ref.queryCandidates.items():
            if phrase in qcsByKeyword:
                qcsByKeyword[phrase].append(qc)
            else:
                qcsByKeyword[phrase] = [qc]

    for phrase, qcs in qcsByKeyword.items():
        qcs.sort(key=lambda qc: qc.asJudgment(), reverse=True)
    return qcsByKeyword

def insertNegativeJudgments(reflections):
    from random import random
    allNp = set()
    for title, ref in reflections.items():
        for phrase, qc in ref.queryCandidates.items():
            allNp.add(qc.qp)

    # Apply each qc to every other qc, add as a negative judgment if not mentioned
    for title, ref in reflections.items():
        for np in allNp:
            if random() < INCLUDE_NEGATIVE:
                ref.addNegativeJudgment(np)


def qcToJudg(qc, qid):
    weight = 1
    # If a naturally occuring term in this doc, weight higher
    return Judgment(grade=qc.asJudgment(),
                    qid=qid,
                    keywords=qc.qp,
                    docId=qc.docId,
                    weight=weight,
                    title=qc.docTitle
                    )



def toJudgList(inverted, minTopGrade=1, minLen=10):
    judgList = []
    qid=0
    for phrase, qcs in inverted.items():
        if len(qcs) >= minLen and qcs[0].asJudgment() >= minTopGrade:
            for qc in qcs:
                judg = qcToJudg(qc, qid=qid)
                judgList.append(judg)
            qid += 1
    return judgList, (qid+1)



if __name__ == "__main__":
    es=Elasticsearch()
    reflections = reflectSeries(es)
    insertNegativeJudgments(reflections=reflections)
    inverted = invertReflections(reflections)
    judgList, numQueries = toJudgList(inverted)
    print("Got %s Good Judgments" % numQueries)
    judgmentsToFile(filename='synth_judg.txt', judgmentsList=judgList)

