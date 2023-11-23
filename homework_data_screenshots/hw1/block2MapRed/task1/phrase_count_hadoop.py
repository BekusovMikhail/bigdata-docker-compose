from mrjob.job import MRJob
from mrjob.step import MRStep

class Count_phrases(MRJob):
    def mapper_1(self, _, line):
        icp = [x for x in line.split('"') if x != "" and x != " "]
        if len(icp) == 3:
            character = icp[1]
            yield (character, 1)
        else:
            pass

    def reducer_1(self, key, values):
        yield key, sum(values)

    def mapper_2(self, key, value):
        yield None, (key, value)

    def reducer_init_2(self):
        self.top_characters = []

    def reducer_2(self, _, values):
        for count, character in values:
            self.top_characters.append((count, character))
            self.top_characters = sorted(
                self.top_characters, reverse=True, key=lambda x: x[1]
            )[:20]

    def reducer_final_2(self):
        for count, character in self.top_characters:
            yield character, count

    def steps(self):
        return [
            MRStep(mapper=self.mapper_1, reducer=self.reducer_1),
            MRStep(
                mapper=self.mapper_2,
                reducer_init=self.reducer_init_2,
                reducer=self.reducer_2,
                reducer_final=self.reducer_final_2,
            ),
        ]


if __name__ == "__main__":
    Count_phrases().run()
