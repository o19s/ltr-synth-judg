import string
import logging
from elasticsearch import Elasticsearch
import spacy

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


# Assemble all noun phrases into queryCandidates
class QueryCandidate:
    def __init__(self, es, value, queryPhrase, docId, natural, minDocFreq=3):
        self.value = value
        self.docId = docId
        self.natural = natural
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

    def __str__(self):
        return "val:%s - pf:%s df:%s idf:%s docId:%s nat:%s" % (self.value,
                self.phraseFreq, self.phraseDf, self.phraseIdf, self.docId, self.natural)


def addNounPhrases(queryCandidates, nPhrases, es, fromDocId, value=1.0, natural=True):
    for np in nPhrases:
        lowerNp = np.lower()
        if lowerNp in queryCandidates:
            queryCandidates[lowerNp].addOccurence(value=value)
        else:
            queryCandidates[lowerNp] = QueryCandidate(es=es, value=value, docId=fromDocId, queryPhrase=lowerNp, natural=natural)
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
                rf = Reflector(es=self.es, docId=self.docId, doc=doc['_source'], stepNo=self.stepNo-1)
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
        allQ = {
            "size": 50,
            "query": {
                "bool": {
                    "must": [
                        {"match": {
                            "belongs_to_collection.id": collId}}
                    ],
                    "must_not": [
                        {"match": {"_id": self.doc['id']}}
                    ]
                }
            }
        }

        resp = self.es.search(index=index, body=allQ)
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

    def __init__(self, doc, es, docId, index='tmdb', stepNo=1):
        logger.info("Phrase Cache Size %s" % len(phraseDocFreq.cache))
        self.stepDocs = set()
        self.doc = doc
        self.docId = docId
        self.es = es
        self.stepNo = stepNo
        self.textQueryCandidates = {}
        self.textTerms = self.textPhrases = []
        if 'overview' not in doc or doc['overview'] is None:
            return

        nlp = spacy.load('en')
        importantText = self.doc['title'] + '\n' + self.doc['overview']

        if 'genres' in self.doc:
            importantText += "\n".join([genre['name'] for genre in self.doc['genres']])

        if 'cast' in self.doc:
            importantText += "\n".join([cast['name'] for cast in self.doc['cast'][:10] ])
            importantText += "\n".join([cast['character'] for cast in self.doc['cast'][:10] ])

        impTextNlp = nlp(importantText)

        nPhrases = [str(np) for np in impTextNlp.noun_chunks]

        # Pull out contiguous proper nouns from chunks, add to list
        nouns = self.contigPosTokSet(nPhrases=nPhrases, nlp=nlp, pos='NOUN')
        propNouns = self.contigPosTokSet(nPhrases=nPhrases, nlp=nlp, pos='PROPN')

        # Always consider the title a proper noun
        propNouns.add(self.doc['title'])

        addNounPhrases(self.textQueryCandidates, nPhrases, fromDocId=self.docId, value=0.5,es=es)
        logger.debug("Adding Bare Nouns %s" % nouns)
        addNounPhrases(self.textQueryCandidates, nPhrases=nouns, fromDocId=self.docId, value=2.5,es=es)
        logger.debug("Adding Prop Nouns %s" % propNouns)
        addNounPhrases(self.textQueryCandidates, nPhrases=propNouns, fromDocId=self.docId, value=10.0,es=es)

        self.stepCollection()
        self.stepTitleMatch()

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
        rfor = Reflector(doc, docId=doc['id'], es=es)
        for queryScore in rfor.queries():
            if queryScore[1] > 10:
                print(" --- %s %s (%s) " % queryScore)


