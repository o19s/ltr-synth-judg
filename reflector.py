import string
import logging
from elasticsearch import Elasticsearch
import spacy
import atexit
import json
import os.path
from enum import Enum

class QueryClass(Enum):
    EXACT_TITLE = 1
    PARTIAL_TITLE = 2
    BODY_PROPER_NOUNS = 10
    BODY_NOUNS = 20
    LINKED_BODY_TERMS = 50
    UNRELATED_TERMS = 1000

nlp = spacy.load('en')

# Enable logging for this module
logger = logging.getLogger('reflector')
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
logger.addHandler(ch)

def phraseFreq(phrase, es, index='tmdb'):
    if phrase == 'it' or phrase == 'they':
        return 100000
    termsQ = {
        "size": 0,
        "query": {
            "match_phrase": {
                "text_all.en": phrase
            }
        }
    }
    resp = es.search(index=index, body=termsQ)
    return resp['hits']['total']



def phraseDocFreq(text, es):
    lookupText = text.lower()
    if lookupText not in phraseDocFreq.cache:
        pf = phraseFreq(phrase=text, es=es)
        #bow = self._bagOfWords(text)
        #minDf = 10000000
        #for w in bow:
        #    if w not in self.termHist:
        #        return 1000
        #    if self.termHist[w] < minDf:
        #        minDf = self.termHist[w]
        phraseDocFreq.cache[lookupText] = pf
        return pf
    else:
        return phraseDocFreq.cache[lookupText]
phraseDocFreq.cache={}
if os.path.exists('df_cache.json'):
    with open('df_cache.json') as f:
        phraseDocFreq.cache = json.load(f)


def collectionLookup(es, collId, docId, index='tmdb'):
    if collId not in collectionLookup.cache:
        collQ = {
            "size": 50,
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
    else:
        resp = collectionLookup.cache[collId]
    print("Collection Cache Size %s" % len(collectionLookup.cache))


    delIdx = False
    for idx, hit in enumerate(resp['hits']['hits']):
        if hit['_id'] == docId:
            delIdx = idx
    del resp['hits']['hits'][delIdx]
    return resp

collectionLookup.cache = {}
if os.path.exists('coll_cache.json'):
    with open('coll_cache.json') as f:
        collectionLookup.cache = json.load(f)


@atexit.register
def dumpCache():
    with open('coll_cache.json', 'w') as f:
        print('writing coll cache')
        json.dump(collectionLookup.cache, f)

    with open('df_cache.json', 'w') as f:
        print('writing pf cache')
        json.dump(phraseDocFreq.cache, f)

# Assemble all noun phrases into queryCandidates
class QueryCandidate:
    def __init__(self, es, queryPhrase, docId, docTitle,
                 queryClass, # A queryClass corresponds to a type of match, with lower
                             # going to more important types of matches
                 queryScore  # A queryScore is a priority-specific scoring system
                             # for arbitrating within this class
                 ):
        self.qp = queryPhrase
        self.docId = docId
        self.docTitle = docTitle
        self.queryScore = queryScore
        self.queryClass = queryClass
        self.tf = self.tfidf = 0

    def addOccurence(self, times=1):
        self.phraseFreq += times
        #if updatedConfidence and updatedConfidence > self.confidence:
        #    self.confidence = updatedConfidence
        #if updatedWeight and updatedWeight > self.weight:
        #    self.weight = updatedWeight

    #def score(self):
    #    # from math import sqrt
    #    return self.confidence * self.phraseFreq * self.phraseIdf # sqrt(self.phraseFreq) * self.phraseIdf * self.value

    def asWeight(self):
        """ How important is it to get this one right? """
        return self.weight

    def asJudgment(self):
        """ How confident are we this is a good result
            for this doc? """
        return int(self.queryScore)

    def __repr__(self):
        return str(self)

    def __str__(self):
        return "%s : class %s : score %s " % (self.qp, self.queryClass, self.queryScore)


def addOtherQueryCandidates(queryCandidates, otherQCandidates):
    for np, qc in otherQCandidates.items():
        lowerNp = np.lower()
        if lowerNp in queryCandidates:
            queryCandidates[lowerNp].addOccurence(times=qc.phraseFreq)
        else:
            from copy import copy
            qc = copy(qc)
            qc.weight /= 2
            queryCandidates[lowerNp] = qc
    return queryCandidates



class Reflector:
    """ given a document, what
        queries would make sense to be strong matches?

        ie take 'Star Wars' - clearly title:"Star Wars" would be
        a reasonable match

        we can then synthesize judgments from this so that the
        keyword 'star wars' matches

        clearly this is not as good as implicit or expert judgments

        """


    def _bagOfWords(self, text):
        table = str.maketrans('', '', string.punctuation)
        text = text.replace(')','').replace('(', '').replace('-', ' ').replace('.', ' ').replace('"', ' ').split()
        text = list(set([w.translate(table).lower() for w in text]))
        return text

    def posTokStream(self, np, nlp, pos='PROPN'):
        for token in nlp(np):
            if token.pos_ == pos:
                yield str(token)
            else:
                yield -1

    def posToks(self, np, nlp, pos='PROPN'):
        propN = []
        for tok in self.posTokStream(np, nlp, pos=pos):
            if tok == -1:
                if propN:
                    yield propN
                propN = []
            else:
                propN.append(tok)
        if propN:
            yield propN

    def contigPosTokSet(self, nPhrases, nlp, pos='PROPN'):
        tokSet = set()
        for np in nPhrases:
            tokSet = tokSet.union([' '.join(pn) for pn in self.posToks(np=np, nlp=nlp, pos=pos)])
        if ' ' in tokSet:
            tokSet.remove(' ')
        return tokSet


    def addStepDocs(self, stepColl, resp):
        for doc in resp['hits']['hits']:
            logger.debug("Step Doc %s" % doc['_source']['title'])
            stepColl[doc['_id']] = doc['_source']


    def stepCollection(self, index='tmdb'):
        if 'belongs_to_collection' not in self.doc or self.doc['belongs_to_collection'] is None:
            logger.info("Not Part of Collection %s" % self.doc['title'])
            return False
        if self.stepNo == 0:
            return False
        logger.debug("Step Into Collection %s" % self.doc['belongs_to_collection']['id'])
        collId = self.doc['belongs_to_collection']['id']
        resp = collectionLookup(es=self.es, collId=collId, docId=self.doc['id'])
        self.addStepDocs(stepColl=self.collDocs, resp=resp)



    def stepExactTitleMatch(self, index='tmdb'):
        """ Step into movies with 100% mm title match"""
        logger.debug("Step Into Full Title Match For %s" % self.doc['title'])
        if self.stepNo == 0:
            return {}
        allQ = {
            "size": 10,
            "query": {
                "bool": {
                    "must": [
                        {"match_phrase": {
                            "title_sent": {
                                "query": "SENTINEL_BEGIN %s SENTINEL_END" % self.doc['title'],
                                "boost": 10000.0}}},
                    ],
                    "must_not": [
                        {"match": {"_id": self.doc['id']}}
                    ]
                }
            }
        }
        resp = self.es.search(index=index, body=allQ)
        self.addStepDocs(stepColl=self.exactTitleDocs, resp=resp)

    def hasPhrase(self, np):
        return np in self.queryCandidates

    def addNegativeJudgment(self, np):
        if not self.hasPhrase(np):
            negQc = QueryCandidate(es=None, queryClass=QueryClass.UNRELATED_TERMS,
                                   queryScore=0.0,
                                   queryPhrase=np,
                                   docId=self.docId, docTitle=self.docTitle)
            self.queryCandidates[np] = negQc

    def __init__(self, doc, es, docTitle, docId, index='tmdb', stepNo=1):
        logger.info("Phrase Cache Size %s" % len(phraseDocFreq.cache))
        self.doc = doc
        self.docTitle = docTitle
        self.docId = docId
        self.es = es
        self.stepNo = stepNo
        self.queryCandidates = {}
        self.textTerms = self.textPhrases = []

        self.collDocs = {}
        self.exactTitleDocs = {}

        # Similar movies needed to make relative
        # Scoring/value decisions
        if stepNo > 0:
            self.stepCollection()
            self.stepExactTitleMatch()

        logger.debug("Adding Title %s" % [self.doc['title']])
        qc = QueryCandidate(es=es, queryClass=QueryClass.EXACT_TITLE, queryScore=20.0,
                            docId=docId,
                            docTitle=docTitle,
                            queryPhrase=self.doc['title'])
        self.queryCandidates[qc.qp] = qc

        bodyText = self.doc['overview']
        bodyTextNlp = nlp(bodyText)
        nPhrases = [str(np) for np in bodyTextNlp.noun_chunks]
        # nouns = self.contigPosTokSet(nPhrases=nPhrases, nlp=nlp, pos='NOUN')
        propNouns = self.contigPosTokSet(nPhrases=nPhrases, nlp=nlp, pos='PROPN')

        # Build reflectors for each step doc
        collRefs = {}
        collQcs = []
        for stepDocId, stepDoc in self.collDocs.items():
            collRefs[stepDocId] = Reflector(es=es, doc=stepDoc,
                                            docTitle=stepDoc['title'],
                                            docId=stepDoc['id'],
                                            stepNo=stepNo-1)

            collQcs.extend([refKeyValue[1] for refKeyValue in collRefs[stepDocId].queryCandidates.items()] )

        for np in propNouns:
            if qc not in self.queryCandidates:
                qc = QueryCandidate(es=es, queryClass=QueryClass.BODY_PROPER_NOUNS, queryScore=1,
                                    docId=docId,
                                    docTitle=docTitle,
                                    queryPhrase=np)
                self.queryCandidates[qc.qp] = qc
                qc.tf = 1
            else:
                qc = self.queryCandidates[np]
                qc.tf += 1

        # Add in linked doc proper nouns to amplify proper nouns here
        for otherQc in collQcs:
            if otherQc.qp in self.queryCandidates and otherQc.queryClass == QueryClass.BODY_PROPER_NOUNS:
                print("Amplifying %s" % otherQc.qp)
                self.queryCandidates[otherQc.qp].tf += (0.7 * otherQc.tf)


        minDocFreq = 3
        maxDocFreq = 100
        for qp, qc in self.queryCandidates.items():
            if qc.queryClass == QueryClass.BODY_PROPER_NOUNS:
                docFreq = phraseDocFreq(es=es, text=qc.qp)
                if docFreq >= minDocFreq and docFreq <= maxDocFreq:
                    if qc.tf >= 2:
                        qc.queryScore = 19
                    else:
                        qc.queryScore = 10
                    qc.tfIdf = qc.tf * (1000 / docFreq)
                else:
                    qc.queryScore = 0

        #if 'overview' not in doc or doc['overview'] is None:
        #    return

        #bodyText = self.doc['title'] + '. \n' + self.doc['overview'] + '\n'

        #genrePhrases = castNamePhrases = charPhrases = []
        #if 'genres' in self.doc:
        #    genrePhrases = [genre['name'] for genre in self.doc['genres']]

        #if 'cast' in self.doc:
        #    castNamePhrases = [cast['name'] for cast in self.doc['cast'][:10] ]
        #    charPhrases = [cast['character'] for cast in self.doc['cast'][:10] ]

        #bodyTextNlp = nlp(bodyText)

        #nPhrases = [str(np) for np in bodyTextNlp.noun_chunks]

        # Pull out contiguous proper nouns from chunks, add to list
        #nouns = self.contigPosTokSet(nPhrases=nPhrases, nlp=nlp, pos='NOUN')
        #propNouns = self.contigPosTokSet(nPhrases=nPhrases, nlp=nlp, pos='PROPN')

        # Arbitrary noun phrases
        #addNounPhrases(self.queryCandidates, nPhrases,
        #               fromDocTitle=docTitle, fromDocId=self.docId,
        #               confidence=0.5, weight=1.0, es=es)

        #logger.debug("Adding Bare Nouns %s" % nouns)
        #addNounPhrases(self.queryCandidates, nPhrases=nouns,
        #               fromDocTitle=docTitle,fromDocId=self.docId,
        #               confidence=2.5, weight=1.0, es=es)
        #logger.debug("Adding Prop Nouns %s" % propNouns)
        #addNounPhrases(self.queryCandidates, nPhrases=propNouns,
        #               fromDocTitle=docTitle, fromDocId=self.docId,
        #               confidence=2.5, weight=2.0, es=es)

        #logger.debug("Adding Genres %s" % genrePhrases)
        #addNounPhrases(self.queryCandidates, nPhrases=genrePhrases,
        #               fromDocTitle=docTitle,fromDocId=self.docId,
        #               confidence=2.5, weight=2.0, es=es)

        #logger.debug("Adding Cast Names %s" % castNamePhrases)
        #addNounPhrases(self.queryCandidates, nPhrases=castNamePhrases,
        #               fromDocTitle=docTitle,fromDocId=self.docId,
        #               confidence=2.5, weight=2.0, es=es)

        #logger.debug("Adding Char Names %s" % charPhrases)
        #addNounPhrases(self.queryCandidates, nPhrases=charPhrases,
        #               fromDocTitle=docTitle,fromDocId=self.docId,
        #               confidence=1.5, weight=2.0, es=es)

        #logger.debug("Adding Title %s" % [self.doc['title']])
        #addNounPhrases(self.queryCandidates, nPhrases=[self.doc['title'].lower()],
        #               fromDocTitle=docTitle,fromDocId=self.docId,
        #               weight=7.0, confidence=10.0,es=es)


    def queries(self):
        # Exact title phrase
        allQs = [(np, qc.score(), "text-value:%s" % qc) for (np, qc) in self.queryCandidates.items()]
        allQs.sort(key = lambda npScored: npScored[1])
        return allQs




def docs(es, titleSearch="star trek", index='tmdb'):
    allQ = {
        "size": 1,
        "query": {
            "match_phrase": {"title": titleSearch}
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
        rfor = Reflector(doc, docTitle=doc['title'], docId=doc['id'], es=es)
        for np, qc in rfor.queryCandidates.items():
            print(qc)

