
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
