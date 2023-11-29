
from mrjob.job import MRJob
from mrjob.step import MRStep

class MRPhrases(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_phrase,
                   reducer=self.reducer_max_phrase),
            MRStep(reducer=self.reducer_sort)
        ]

    def mapper_get_phrase(self, _, line):
        splitted = line.split('" "')
        if len(splitted) < 3:
            return ('', '')
        _, character, phrase = splitted
        yield (character, phrase[:-1])

    def reducer_max_phrase(self, character, phrases):
        yield None, (character, max(phrases, key=len))

    def reducer_sort(self, _, characters_phrases):
        sorted_phrases = sorted(characters_phrases, key=lambda x: -len(x[1]))
        for character, phrase in sorted_phrases:
            if character == '':
                continue
            yield (character, phrase)

if __name__ == '__main__':
    MRPhrases.run()
