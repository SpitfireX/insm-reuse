import itertools

from collections import Counter
from collections.abc import Iterable

import spacy
import numpy as np

from nltk.util import ngrams
from spacy.strings import hash_string as spacy_hash_string

class ModifiedNgramSimilarity():

    def __init__(self, language):
        self.language = language

        if language == "en":
            from nltk.corpus import wordnet as wn
            self.wn = wn
            self.nlp = spacy.load("en_core_web_sm", disable=["parser", "ner"])
        elif language == "de":
            from germanetpy import germanet
            self.wn = germanet.Germanet("../ressourcen/GermaNet/GN_V140/GN_V140_XML/")
            self.nlp = spacy.load("de_core_news_sm", disable=["parser", "ner"])
        else:
            raise ValueError(f"Unsupported language {language}")

        self.nlp.max_length = 2_000_000

        self.counter_cache = dict()
        self.synset_cache = dict()


    def flatten_multilemma_ngrams(self, ngrams):
        """Takes ngrams that may contain a list of token options in every place
        and flattenes them."""
        
        for ngram in ngrams:
            if not list in map(type, ngram):
                yield ngram
            else:
                ngram = [[t] if type(t) != list else t for t in ngram]
                for new_ngram in itertools.product(*ngram):
                    yield new_ngram

    
    def skipgrams(self, ngrams):
        for ngram in ngrams:
            yield tuple([ ngram[:i] + ngram[i+1:] for i in range(1, len(ngram)-1) ])
            

    def get_lemma_synset(self, token):
        if self.language == "en":
            posmap = {
                "NOUN": "n",
                "VERB": "v",
                "ADJ": "a",
                "ADV": "r",
            }

            wnpos = posmap[token.pos_]
            ss = self.wn.synsets(token.lemma_, wnpos)
            if len(ss) == 1:
                lemmaset = {l.name() for l in ss[0].lemmas() if not "_" in l.name() and l.name() != token.lemma_}
            else:
                lemmaset = {s.lemmas()[0].name() for s in ss if not "_" in s.lemmas()[0].name() and s.lemmas()[0].name() != token.lemma_}

            return list(lemmaset)[:10]
        elif self.language == "de":
            posmap = {
                "NOUN": "nomen",
                "VERB": "verben",
                "ADJ": "adj",
                "ADV": None,
            }

            wnpos = posmap[token.pos_]

            if not wnpos:
                return [token.lemma_]
            else:
                ss = self.wn.get_synsets_by_orthform(token.lemma_)
                
                if len(ss) == 1 and ss[0].word_category.name == wnpos:
                    output = {lu.orthform for lu in ss[0].lexunits if lu.orthform != token.lemma_}
                else:
                    output = set()
                    for s in ss:
                        if s.word_category.name == wnpos:
                            for lu in s.lexunits:
                                if lu.orthform != token.lemma_:
                                    output.add(lu.orthform)

                return list(output)[:10]


    def modified_ngrams(self, ngram):
        mod = list(self.lemmatize([ngram]))

        # synonyms
        synprot = []
        for t in ngram:
            if t.pos_ in ["NOUN", "VERB", "ADJ", "ADV"]:
                if not (t.lemma_, t.pos_) in self.synset_cache:
                    self.synset_cache[(t.lemma_, t.pos_)] = self.get_lemma_synset(t)
                ss = self.synset_cache[(t.lemma_, t.pos_)]
                if ss:
                    synprot.append([spacy_hash_string(l) for l in ss])
                else:
                    synprot.append(t.lemma)
            else:
                synprot.append(t.lemma)
        
        mod.append(tuple(synprot))

        mod = list(self.flatten_multilemma_ngrams(mod))

        # skipgrams
        if len(ngram) > 1:
            skipgrams = list(self.skipgrams(mod))
            mod.extend(skipgrams)

        return mod


    def lemmatize(self, ngrams):
        for ngram in ngrams:
            yield tuple(t.lemma for t in ngram)


    def exp_count(self, ngram, doc):
        mod = self.modified_ngrams(ngram)
        
        if not id(doc) in self.counter_cache:
            self.counter_cache[id(doc)] = Counter(doc)
        acount = self.counter_cache[id(doc)]
        
        result = 0

        for modgram in mod:
            result += acount[modgram]

        return result


    def mod_containment_score(self, angrams, bngrams, bngrams_lemma):
        """scores two seqences of ngrams for their modified ngram-containment"""

        total_contain = 0
        total_bcount = 0

        if not id(bngrams_lemma) in self.counter_cache:
            self.counter_cache[id(bngrams_lemma)] = Counter(bngrams_lemma)
        bcount_lemma = self.counter_cache[id(bngrams_lemma)]

        for ngram, ngram_lemma in zip(bngrams, bngrams_lemma):
            total_contain += min(self.exp_count(ngram, angrams), bcount_lemma[ngram_lemma])
            total_bcount += bcount_lemma[ngram_lemma]
        if total_bcount != 0:
            return total_contain / total_bcount
        else:
            return 0


    def score_texts(self, texta, textb, ngram_length=5):
        """scores two texts with different ngram-containment-metrics"""

        doca_lemma = [t.lemma for t in self.nlp(texta)] # texta only needs to be in lemma form

        docb = list(self.nlp(textb))
        docb_lemma = [t.lemma for t in self.nlp(textb)]

        seqs = dict()
        scores = dict()
        acat = []

        for n in range(1, ngram_length+1):
            acat.extend(list(ngrams(doca_lemma, n)))

        for n in range(1, ngram_length+1):
            seqs[f"{n}_mod"] = (list(ngrams(docb, n)), list(ngrams(docb_lemma, n)))

        for key, (b, b_lemma) in seqs.items():
            scores[key] = self.mod_containment_score(acat, b, b_lemma)
        
        return scores
