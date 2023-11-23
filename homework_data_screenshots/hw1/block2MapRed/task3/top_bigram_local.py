from mrjob.job import MRJob
from mrjob.step import MRStep
from nltk import word_tokenize
from nltk.corpus import stopwords
import re
from nltk.stem.snowball import SnowballStemmer

# Uncomment if not installed. If install comment, because nltk.download is bugged
import nltk
# nltk.download('stopwords')
# nltk.download('punkt')

class Top_bigram(MRJob):
    def mapper_1_init(self):
        self.stopwords = set(stopwords.words("english")) # не работает на хадупе ни в какую...
        self.stemmer = SnowballStemmer("english")
        self.tokenizer = word_tokenize
        pass
    def mapper_1(self, _, line):
        icp = [x for x in line.split('"') if x != "" and x != " "]
        if len(icp) == 3:
            phrase = icp[2]
            cleaned_line = re.sub(r"[^\w\s]", "", phrase.lower())
            tokenized_line = word_tokenize(cleaned_line)
            words = [self.stemmer.stem(x) for x in tokenized_line if x not in self.stopwords]
            bigrams = [
                f"{words[i]} {words[i+1]}" for i in range(len(words) - 1)
            ]
            for bigram in bigrams:
                yield bigram, 1
        else:
            pass

    def reducer_1(self, bigram, counts):
        yield None, (bigram, sum(counts))

    def reducer_2(self, _, bigram_counts):
        sorted_bigrams = sorted(
            bigram_counts, key=lambda x: x[1], reverse=True
        )[:20]
        for bigram, count in sorted_bigrams:
            yield bigram, count

    def steps(self):
        return [
            MRStep(mapper_init=self.mapper_1_init, mapper=self.mapper_1, reducer=self.reducer_1),
            MRStep(reducer=self.reducer_2),
        ]

if __name__ == "__main__":
    top_bigram = Top_bigram()
    with top_bigram.make_runner() as runner:
        runner.run()
        output = [
            output for output in top_bigram.parse_output(runner.cat_output())
        ]
    for x in output:
        print(x)
