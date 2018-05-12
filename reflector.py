import logging
from elasticsearch import Elasticsearch
from enum import Enum
from phraseStats import phraseDocFreq
from collStats import collectionLookup
from queryCandidate import QueryCandidate
from posParser import PhraseExtractor
from movieDoc import byTitlePhrase
from popularity import moviePopularity

class QueryClass(Enum):
    EXACT_TITLE = 1
    PARTIAL_TITLE = 2
    COLLECTION_TITLE = 5
    BODY_PROPER_NOUNS = 10
    BODY_NOUNS = 20
    LINKED_BODY_TERMS = 50
    UNRELATED_TERMS = 1000


# Enable logging for this module
logger = logging.getLogger('reflector')
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
logger.addHandler(ch)



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
        collId = self.doc['belongs_to_collection']['id']
        resp, minPop, maxPop = collectionLookup(es=self.es,
                                                        collId=collId,
                                                        docId=self.doc['id'])
        self.addStepDocs(stepColl=self.collDocs, resp=resp)
        logger.debug("Found %s Collection Matches For %s" % (resp['hits']['total'], self.doc['title']))
        self.maxCollPop = maxPop
        self.minCollPop = minPop



    def stepExactTitleMatch(self, index='tmdb'):
        """ Step into movies with 100% mm title match"""
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
        if resp['hits']['total'] > 0:
            import pdb; pdb.set_trace()
        logger.debug("Found %s Full Title Matches For %s" % (resp['hits']['total'], self.doc['title']))

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
        self.doc = doc
        self.docTitle = docTitle
        self.docId = docId
        self.es = es
        self.docPop = moviePopularity(doc)
        self.maxCollPop = moviePopularity(doc)
        self.minCollPop = moviePopularity(doc) - 0.01
        self.stepNo = stepNo
        self.queryCandidates = {}
        self.textTerms = self.textPhrases = []

        self.collDocs = {}
        self.exactTitleDocs = {}

        self.phrases = PhraseExtractor.create(doc['overview'])

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


        # Build reflectors for each step doc
        collRefs = {}
        collQcs = []
        if stepNo > 0:
            logger.debug("**Recursing Into CollDocs %s!" % stepNo)
            for stepDocId, stepDoc in self.collDocs.items():
                collRefs[stepDocId] = Reflector(es=es, doc=stepDoc,
                                                docTitle=stepDoc['title'],
                                                docId=stepDoc['id'],
                                                stepNo=stepNo-1)

                collQcs.extend([refKeyValue[1] for refKeyValue in collRefs[stepDocId].queryCandidates.items()] )
            logger.debug("**POP FROM CollDocs %s!" % stepNo)


        # Add titles of sibling collections here
        for stepDocId, stepDoc in self.collDocs.items():
            if 'title' in stepDoc:
                stepDocPop = moviePopularity(stepDoc)
                stepDocTitle = stepDoc['title']
                # Process movie titles in teh same collection
                logger.debug("Adding Collection Sibling Title %s vote/min/max %s/%s/%s" % (
                    stepDoc['title'], stepDocPop, self.minCollPop, self.maxCollPop))
                queryScore = 17
                if (self.maxCollPop - self.minCollPop) > 0:
                    voteSpread = (stepDocPop - self.minCollPop) \
                                    / (self.maxCollPop - self.minCollPop)
                    if voteSpread >= 0 and voteSpread <= 1:
                        queryScore += int((voteSpread) * 2)

                qc = QueryCandidate(es=es, queryClass=QueryClass.COLLECTION_TITLE,
                                    queryScore=queryScore,
                                    docId=docId,
                                    docTitle=docTitle,
                                    queryPhrase=stepDocTitle)
                if qc.qp not in self.queryCandidates:
                    self.queryCandidates[qc.qp] = qc

        # Process proper nouns that occur here
        for np in self.phrases.propNouns:
            logger.debug("Prop Noun Discovered %s" % (qc.qp))
            if np not in self.queryCandidates:
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
                self.queryCandidates[otherQc.qp].tf += (1.0 * otherQc.tf)


        minDocFreq = 2
        maxDocFreq = 100
        deletePhrases = set()
        for qp, qc in self.queryCandidates.items():
            if qc.queryClass == QueryClass.BODY_PROPER_NOUNS:
                docFreq, minPop, maxPop = phraseDocFreq(es=es, text=qc.qp)
                voteSpread = min(1, (self.docPop / 7.5) * ((self.docPop - minPop) / (maxPop - minPop)))
                logger.debug("Phrase %s tf/df/minDf/maxDf :%s/%s/%s/%s | %s/%s/%s => %s" % (qc.qp, qc.tf, docFreq, minDocFreq, maxDocFreq, minPop, self.docPop, maxPop, voteSpread))
                if docFreq >= minDocFreq and docFreq <= maxDocFreq:
                    if qc.tf >= 2:
                        qc.queryScore = 10
                        if voteSpread >= 0 and voteSpread <= 1:
                            qc.queryScore += int(6 * (voteSpread))
                    else:
                        qc.queryScore = 10
                    qc.tfIdf = qc.tf * (1000 / docFreq)
                else:
                    logger.debug("Phrase %s out of docfreq range" % qc.qp)
                    deletePhrases.add(qc.qp)
                    qc.queryScore = -1

        if stepNo >= 1:
            for deleteQuery in deletePhrases:
                logger.info("DELETING PROPER NOUN %s" % deleteQuery)
                del self.queryCandidates[deleteQuery]

        logger.info("Done Building Me! \n %s " % self)

    def __str__(self):
        rVal = ""
        for np, qc in self.queryCandidates.items():
            rVal += "%s\n" % qc

        return rVal



    def queries(self):
        # Exact title phrase
        allQs = [(np, qc.score(), "text-value:%s" % qc) for (np, qc) in self.queryCandidates.items()]
        allQs.sort(key = lambda npScored: npScored[1])
        return allQs




if __name__ == "__main__":
    es = Elasticsearch()
    from sys import argv
    for doc in byTitlePhrase(titleSearch=argv[1], es=es):
        print(doc['title'])
        print(doc['id'])
        print(doc['overview'])
        rfor = Reflector(es=es, doc=doc, docTitle=doc['title'], docId=doc['id'], index='tmdb')
        for np, qc in rfor.queryCandidates.items():
            print(qc)

