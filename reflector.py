import string
import logging
from elasticsearch import Elasticsearch
import spacy
import atexit
import json
import os.path

nlp = spacy.load('en')

# Enable logging for this module
logger = logging.getLogger('reflector')
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
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
    def __init__(self, es, value, queryPhrase, docId, docTitle, natural, minDocFreq=3):
        self.value = value
        self.qp = queryPhrase
        self.docId = docId
        self.docTitle = docTitle
        self.natural = natural
        self.phraseDf = 0
        if value > 0:
            self.phraseDf = phraseDocFreq(queryPhrase, es=es)
        self.phraseIdf = 0
        if self.phraseDf >= minDocFreq:
            self.phraseIdf = 1000.0 / (self.phraseDf + 1)
        self.phraseFreq = 1

    def addOccurence(self, value=None, times=1):
        self.phraseFreq += times
        if value:
            self.value = value

    def score(self):
        # from math import sqrt
        return self.value * self.phraseFreq * self.phraseIdf # sqrt(self.phraseFreq) * self.phraseIdf * self.value

    def asJudgment(self):
        score = self.score()
        if score >= 500 and self.natural:
            return 4
        elif score >= 1500:
            return 4
        elif score > 100:
            return 3
        elif score > 0:
            return 1
        else:
            return 0

    def __repr__(self):
        return str(self)

    def __str__(self):
        return "val:%s - pf:%s df:%s idf:%s docId:%s(%s) nat:%s judg:%s" % (self.value,
                self.phraseFreq, self.phraseDf, self.phraseIdf, self.docId, self.docTitle, self.natural, self.asJudgment())


def addNounPhrases(queryCandidates, nPhrases, es, fromDocId, fromDocTitle, value=1.0, natural=True):
    for np in nPhrases:
        lowerNp = np.lower()
        if lowerNp in queryCandidates:
            queryCandidates[lowerNp].addOccurence(value=value)
        else:
            queryCandidates[lowerNp] = QueryCandidate(es=es, value=value,
                                                      docId=fromDocId, docTitle=fromDocTitle,
                                                      queryPhrase=lowerNp, natural=natural)
    return queryCandidates

def addOtherQueryCandidates(queryCandidates, otherQCandidates):
    for np, qc in otherQCandidates.items():
        lowerNp = np.lower()
        if lowerNp in queryCandidates:
            queryCandidates[lowerNp].addOccurence(times=qc.phraseFreq)
        else:
            from copy import copy
            qc = copy(qc)
            qc.natural = False
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


    def addStepDocs(self, resp):
        for doc in resp['hits']['hits']:
            # Dont double add docs through various similarity methods
            if doc['_id'] not in self.stepDocs:
                self.stepDocs.add(doc['_id'])
                rf = Reflector(es=self.es, docTitle=self.docTitle, docId=self.docId, doc=doc['_source'], stepNo=self.stepNo-1)
                logger.info("Stepped To %s" % doc['_source']['title'])
                addOtherQueryCandidates(self.textQueryCandidates, rf.textQueryCandidates)


    def stepCollection(self, index='tmdb'):
        if 'belongs_to_collection' not in self.doc or self.doc['belongs_to_collection'] is None:
            logger.info("Not Part of Collection %s" % self.doc['title'])
            return False
        if self.stepNo == 0:
            return False
        logger.debug("Step Into Collection %s" % self.doc['belongs_to_collection']['id'])
        collId = self.doc['belongs_to_collection']['id']
        resp = collectionLookup(es=self.es, collId=collId, docId=self.doc['id'])
        self.addStepDocs(resp)



    def stepTitleMatch(self, index='tmdb'):
        """ Step into movies with 100% mm title match"""
        logger.debug("Step Into Full Title Match For %s" % self.doc['title'])
        if self.stepNo == 0:
            return {}
        allQ = {
            "size": 10,
            "query": {
                "bool": {
                    "must": [
                        {"match": {
                            "title": {
                                "minimum_should_match": "100%",
                                "query": self.doc['title'],
                                "boost": 10000.0}}},
                    ],
                    "must_not": [
                        {"match": {"_id": self.doc['id']}}
                    ]
                }
            }
        }
        resp = self.es.search(index=index, body=allQ)
        self.addStepDocs(resp)

    def hasPhrase(self, np):
        return np in self.textQueryCandidates

    def addNegativeJudgment(self, np):
        if not self.hasPhrase(np):
            negQc = QueryCandidate(es=None,  value=0.0,
                                   natural=False, queryPhrase=np,
                                   docId=self.docId, docTitle=self.docTitle)
            self.textQueryCandidates[np] = negQc

    def __init__(self, doc, es, docTitle, docId, index='tmdb', stepNo=1):
        logger.info("Phrase Cache Size %s" % len(phraseDocFreq.cache))
        self.stepDocs = set()
        self.doc = doc
        self.docTitle = docTitle
        self.docId = docId
        self.es = es
        self.stepNo = stepNo
        self.textQueryCandidates = {}
        self.textTerms = self.textPhrases = []
        if 'overview' not in doc or doc['overview'] is None:
            return

        bodyText = self.doc['title'] + '. \n' + self.doc['overview'] + '\n'

        genrePhrases = castNamePhrases = charPhrases = []
        if 'genres' in self.doc:
            genrePhrases = [genre['name'] for genre in self.doc['genres']]

        if 'cast' in self.doc:
            castNamePhrases = [cast['name'] for cast in self.doc['cast'][:10] ]
            charPhrases = [cast['character'] for cast in self.doc['cast'][:10] ]

        bodyTextNlp = nlp(bodyText)

        nPhrases = [str(np) for np in bodyTextNlp.noun_chunks]

        # Pull out contiguous proper nouns from chunks, add to list
        nouns = self.contigPosTokSet(nPhrases=nPhrases, nlp=nlp, pos='NOUN')
        propNouns = self.contigPosTokSet(nPhrases=nPhrases, nlp=nlp, pos='PROPN')

        # Always consider the title a proper noun
        addNounPhrases(self.textQueryCandidates, nPhrases,
                       fromDocTitle=docTitle, fromDocId=self.docId, value=0.5,es=es)
        logger.debug("Adding Bare Nouns %s" % nouns)
        addNounPhrases(self.textQueryCandidates, nPhrases=nouns,
                       fromDocTitle=docTitle,fromDocId=self.docId, value=2.5,es=es)
        logger.debug("Adding Prop Nouns %s" % propNouns)
        addNounPhrases(self.textQueryCandidates, nPhrases=propNouns,
                       fromDocTitle=docTitle, fromDocId=self.docId, value=10.0,es=es)

        logger.debug("Adding Genres %s" % genrePhrases)
        addNounPhrases(self.textQueryCandidates, nPhrases=genrePhrases,
                       fromDocTitle=docTitle,fromDocId=self.docId, value=5.0,es=es)

        logger.debug("Adding Cast Names %s" % castNamePhrases)
        addNounPhrases(self.textQueryCandidates, nPhrases=castNamePhrases,
                       fromDocTitle=docTitle,fromDocId=self.docId, value=1.5,es=es)

        logger.debug("Adding Char Names %s" % charPhrases)
        addNounPhrases(self.textQueryCandidates, nPhrases=charPhrases,
                       fromDocTitle=docTitle,fromDocId=self.docId, value=1.5,es=es)

        logger.debug("Adding Title %s" % [self.doc['title']])
        addNounPhrases(self.textQueryCandidates, nPhrases=[self.doc['title']],
                       fromDocTitle=docTitle,fromDocId=self.docId, value=10.0,es=es)

        self.stepCollection()
        # self.stepTitleMatch()

    def queries(self):
        # Exact title phrase
        allQs = [(np, qc.score(), "text-value:%s" % qc) for (np, qc) in self.textQueryCandidates.items()]
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
        for queryScore in rfor.queries():
            if queryScore[1] > 10:
                print(" --- %s %s (%s) " % queryScore)


