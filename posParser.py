import spacy

nlp = spacy.load('en')

class NullPhraseExtractor:

    def __init__(self):
        self.propNouns = set()


class PhraseExtractor:


    def create(text):
        if isinstance(text, str):
            return PhraseExtractor(text)
        return NullPhraseExtractor()


    def __init__(self, text):
        try:
            self.bodyTextNlp = nlp(text)
            self.nPhrases = [str(np) for np in self.bodyTextNlp.noun_chunks]
            # nouns = self.contigPosTokSet(nPhrases=nPhrases, nlp=nlp, pos='NOUN')
            self.propNouns = self._contigPosTokSet(nPhrases=self.nPhrases, nlp=nlp, pos='PROPN')
        except TypeError as e:
            print(e)
            import pdb; pdb.set_trace()

    def _posTokStream(self, np, nlp, pos='PROPN'):
        for token in nlp(np):
            if token.pos_ == pos:
                yield str(token)
            else:
                yield -1

    def _posToks(self, np, nlp, pos='PROPN'):
        propN = []
        for tok in self._posTokStream(np, nlp, pos=pos):
            if tok == -1:
                if propN:
                    yield propN
                propN = []
            else:
                propN.append(tok)
        if propN:
            yield propN

    def _contigPosTokSet(self, nPhrases, nlp, pos='PROPN'):
        tokSet = set()
        for np in nPhrases:
            tokSet = tokSet.union([' '.join(pn) for pn in self._posToks(np=np, nlp=nlp, pos=pos)])
        if ' ' in tokSet:
            tokSet.remove(' ')
        return tokSet

