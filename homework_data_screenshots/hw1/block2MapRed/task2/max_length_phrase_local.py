from mrjob.job import MRJob
from mrjob.step import MRStep


class Max_length_phrase(MRJob):
    def mapper_1(self, _, line):
        icp = [x for x in line.split('"') if x != "" and x != " "]
        if len(icp) == 3:
            character = icp[1]
            phrase = icp[2]
            yield character, (len(phrase), phrase)
        else:
            pass

    def reducer_1(self, person, values):
        max_length = 0
        longest_phrase = None
        for length, phrase in values:
            if length > max_length:
                max_length = length
                longest_phrase = phrase
        yield None, (person, max_length, longest_phrase)

    def reducer_2(self, _, values):
        results = list(values)
        results.sort(key=lambda x: x[1], reverse=True)
        for person, length, phrase in results:
            yield person, phrase

    def steps(self):
        return [
            MRStep(mapper=self.mapper_1, reducer=self.reducer_1),
            MRStep(reducer=self.reducer_2),
        ]


if __name__ == "__main__":
    max_length_phrase = Max_length_phrase()
    with max_length_phrase.make_runner() as runner:
        runner.run()
        output = [
            output for output in max_length_phrase.parse_output(runner.cat_output())
        ]
    for x in output:
        print(x)
