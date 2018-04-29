import string
from elasticsearch import Elasticsearch

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
    pf = phraseFreq(phrase=text, es=es)
    #bow = self._bagOfWords(text)
    #minDf = 10000000
    #for w in bow:
    #    if w not in self.termHist:
    #        return 1000
    #    if self.termHist[w] < minDf:
    #        minDf = self.termHist[w]
    return pf


# Assemble all noun phrases into queryCandidates
class QueryCandidate:
    def __init__(self, es, value, queryPhrase, minDocFreq=3):
        self.value = value
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
        return "val:%s - pf:%s df:%s idf:%s" % (self.value,
                self.phraseFreq, self.phraseDf, self.phraseIdf)


def addNounPhrases(queryCandidates, nPhrases, es, value=1.0):
    for np in nPhrases:
        lowerNp = np.lower()
        if lowerNp in queryCandidates:
            queryCandidates[lowerNp].addOccurence(value=value)
        else:
            queryCandidates[lowerNp] = QueryCandidate(es=es, value=value, queryPhrase=lowerNp)
    return queryCandidates

def addOtherQueryCandidates(queryCandidates, otherQCandidates):
    for np, qc in otherQCandidates.items():
        lowerNp = np.lower()
        if lowerNp in queryCandidates:
            queryCandidates[lowerNp].addOccurence(times=qc.phraseFreq)
        else:
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


    def getTermHist(self, es, index='tmdb'):
        termsQ = {
            "size": 0,
            "query": {
                "match_all": {}
            },
            "aggs": {
               "all_terms": {
                   "terms": {
                      "field": "text_all"
                      , "size": "40000"
                   }
               }
            }
        }
        resp = es.search(index=index, body=termsQ)
        terms =  resp['aggregations']['all_terms']['buckets']
        termDict = {}
        for term in terms:
            termDict[term['key']] = term['doc_count']

        return termDict


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


    def step(self, index='tmdb'):
        if self.stepNo == 0:
            return {}
        topN = ["\"%s\"^%s" % (np, qc.score())
                 for (np, qc) in self.overviewQueryCandidates.items()]

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
                    "should": [
                        {"query_string": {"query": " ".join(topN), "fields": ["text_all.en"]}}
                    ],
                    "must_not": [
                        {"match": {"_id": self.doc['id']}}
                    ]
                }
            }
        }
        resp = es.search(index=index, body=allQ)
        for doc in resp['hits']['hits']:
            rf = Reflector(es=self.es, doc=doc['_source'], stepNo=self.stepNo-1)
            print("%s" % doc['_source']['title'])
            addOtherQueryCandidates(self.overviewQueryCandidates, rf.overviewQueryCandidates)

    def __init__(self, doc, es, stepNo=1):
        self.doc = doc
        self.es = es
        self.stepNo = stepNo
        self.overviewQueryCandidates = {}
        # self.termHist = self.getTermHist(es)
        self.overviewTerms = self.overviewPhrases = []
        if 'overview' not in doc or doc['overview'] is None:
            return
        self.overviewTerms = self._bagOfWords(self.doc['overview'])
        # self.overviewTerms.sort(key=lambda term: self.termHist[term] if term in self.termHist else -1)
        # Map each overview term to a doc freq

        #print("Terms")
        #for term in self.overviewTerms:
        #    if term in self.termHist:
        #        print ("term %s -> %s" % (term, self.termHist[term]) )
        #    else:
        #        print ("term %s -> %s" % (term, -1) )

        #print("Noun Phrases (overview)")
        #print("===============")
        import spacy
        nlp = spacy.load('en')
        overview = nlp(self.doc['overview'])

        nPhrases = [str(np) for np in overview.noun_chunks]

        # Pull out contiguous proper nouns from chunks, add to list
        nouns = self.contigPosTokSet(nPhrases=nPhrases, nlp=nlp, pos='NOUN')
        propNouns = self.contigPosTokSet(nPhrases=nPhrases, nlp=nlp, pos='PROPN')

        addNounPhrases(self.overviewQueryCandidates, nPhrases, value=0.5,es=es)
        print("Adding Bare Nouns %s" % nouns)
        addNounPhrases(self.overviewQueryCandidates, nPhrases=nouns, value=2.5,es=es)
        print("Adding Prop Nouns %s" % propNouns)
        addNounPhrases(self.overviewQueryCandidates, nPhrases=propNouns, value=10.0,es=es)

        self.step()

    def queries(self):
        # Exact title phrase
        allQs = [(np, qc.score(), "overview-value:%s" % qc) for (np, qc) in self.overviewQueryCandidates.items()]
        allQs.extend([(self.doc['title'], 10000, 'title')])
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
        rfor = Reflector(doc, es=es)
        for queryScore in rfor.queries():
            if queryScore[1] > 1:
                print(" --- %s %s (%s) " % queryScore)



