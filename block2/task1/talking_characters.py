
from mrjob.job import MRJob
from mrjob.step import MRStep

class MRMostTalkingCharacters(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_characters,
                   reducer=self.reducer_count_characters),
            MRStep(reducer=self.reducer_find_max_characters)
        ]

    def mapper_get_characters(self, _, line):
        character = line.split('" "')[1]
        yield (character, 1)

    def reducer_count_characters(self, character, counts):
        yield None, (character, sum(counts))

    def reducer_find_max_characters(self, _, characters_count_pairs):
        best_characters = sorted(characters_count_pairs, key=lambda x: -x[1])[:20]
        for character, count in best_characters:
            yield (character, count)

if __name__ == '__main__':
    MRMostTalkingCharacters.run()
