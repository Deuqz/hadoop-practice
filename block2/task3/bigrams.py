
from mrjob.job import MRJob
from mrjob.step import MRStep
import nltk

class MRBigrams(MRJob):
    def mapper_init(self):
        nltk.download('punkt')
        nltk.download('stopwords')

    def steps(self):
        return [
            MRStep(mapper_init=self.mapper_init,
                   mapper=self.mapper_get_bigrams,
                   reducer=self.reducer_count_bigrams),
            MRStep(reducer=self.reducer_find_best_bigrams)
        ]

    def mapper_get_bigrams(self, _, line):
        splitted = line.split('" "')
        if len(splitted) < 3:
            yield ('', 0)
        else:
            phrase = splitted[2]
            phrase = phrase[:-1]
            words = nltk.word_tokenize(phrase)
            words=[word.lower() for word in words if word.isalpha()]
            for word in words:
                for bigram in nltk.bigrams(word):
                    yield (bigram[0] + bigram[1], 1)

    def reducer_count_bigrams(self, bigram, counts):
        yield None, (bigram, sum(counts))

    def reducer_find_best_bigrams(self, _, bigram_count_pairs):
        best_bigrams = sorted(bigram_count_pairs, key=lambda x: -x[1])[:20]
        for bigram, count in best_bigrams:
            yield (bigram, count)

if __name__ == '__main__':
    MRBigrams.run()
