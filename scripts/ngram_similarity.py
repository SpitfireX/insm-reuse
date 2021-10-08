import spacy
import numpy as np

from nltk.util import ngrams

class NgramSimilarity():

    def __init__(self, language):
        if language == "de":
            self.nlp = spacy.load("de_core_news_sm", disable=["parser", "ner"])
        elif language == "en":
            self.nlp = spacy.load("en_core_web_sm", disable=["parser", "ner"])
        else:
            raise ValueError(f"Unsupported language {language}")
        
        self.nlp.max_length = 2_000_000
        self.language = language

    def make_sets(self, tokens, n=5):
        """creation of different sets per text:
        sets of ngrams with len 1, 3, 5
        set of hapax legomena"""

        lemmas = [t.lemma for t in tokens]
        sets = dict()

        sets[1] = set(lemmas)

        if n > 1:
            for n in range(2, n+1):
                grams = ngrams(lemmas, n)
                sets[n] = set(grams)

        unique = np.unique(np.array(lemmas, dtype='uint64'), return_counts=True)
        sets['hapax'] = set([lemma for lemma, count in zip(*unique) if count == 1])

        return sets

    def setscore(self, seta, setb):
        """scores two sets of lemmas for their ngram-containment"""
        
        if len(seta) == 0 or len(setb) == 0:
            return 0

        overlap = seta & setb
        return len(overlap) / len(setb)

    def score_texts(self, texta, textb, ngram_length=5):
        """scores two texts with different ngram-containment-metrics"""

        setsa = self.make_sets(self.nlp(texta), ngram_length)
        setsb = self.make_sets(self.nlp(textb), ngram_length)

        scores = dict()

        for (akey, a), (bkey, b) in zip(setsa.items(), setsb.items()):
            assert(akey == bkey)
            scores[akey] = self.setscore(a, b)
        
        return scores
