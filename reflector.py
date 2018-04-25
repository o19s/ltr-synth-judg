import string
import re

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


    def phraseFreq(self, phrase, es, index='tmdb'):
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


    def _bagOfWords(self, text):
        table = str.maketrans('', '', string.punctuation)
        text = text.replace('-', ' ').replace('.', ' ').replace('"', ' ').split()
        text = list(set([w.translate(table).lower() for w in text]))
        return text

    def properNounStream(self, np, nlp):
        for token in nlp(np):
            if token.pos_ == 'PROPN':
                yield str(token)
            else:
                yield -1

    def properNounTok(self, np, nlp):
        propN = []
        for tok in self.properNounStream(np, nlp):
            if tok == -1:
                if propN:
                    yield propN
                propN = []
            else:
                propN.append(tok)
        yield propN


    def __init__(self, doc, es):
        self.doc = doc
        self.termHist = self.getTermHist(es)
        self.overviewTerms = self.overviewPhrases = []
        if 'overview' not in doc or doc['overview'] is None:
            return
        self.overviewTerms = self._bagOfWords(self.doc['overview'])
        self.overviewTerms.sort(key=lambda term: self.termHist[term] if term in self.termHist else -1)
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

        def npScore(text):
            pf = self.phraseFreq(text, es=es)
            #bow = self._bagOfWords(text)
            #minDf = 10000000
            #for w in bow:
            #    if w not in self.termHist:
            #        return 1000
            #    if self.termHist[w] < minDf:
            #        minDf = self.termHist[w]
            return (1000 / (pf + 1))

        nPhrases = [str(np) for np in overview.noun_chunks]

        # Pull out proper nouns from chunks, add to list
        propNouns = set()
        for np in nPhrases:
            thisPns = [' '.join(pn) for pn in self.properNounTok(np=np, nlp=nlp)]

            for pn in thisPns:
                if pn:
                    propNouns.add(pn)

        # print("%s" % propNouns)

        self.overviewPhrases = {}
        for np in nPhrases:
            lowerNp = np.lower()
            scored = npScore(np)
            if scored != -1:
                if lowerNp in self.overviewPhrases:
                    self.overviewPhrases[lowerNp] += npScore(np)
                else:
                    self.overviewPhrases[lowerNp] = npScore(np)

        for np in propNouns:
            lowerNp = np.lower()
            scored = npScore(np)
            if scored != -1:
                if lowerNp in self.overviewPhrases:
                    self.overviewPhrases[lowerNp] += npScore(np) * 3
                else:
                    self.overviewPhrases[lowerNp] = npScore(np) * 3

    def queries(self):
        # Exact title phrase
        allQs = [(np, score) for (np, score) in self.overviewPhrases.items()]
        allQs.extend([(self.doc['title'], 10000)])
        allQs.sort(key = lambda npScored: npScored[1])
        return allQs




