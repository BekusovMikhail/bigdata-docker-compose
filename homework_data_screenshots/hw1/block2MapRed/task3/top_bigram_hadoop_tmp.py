#!/usr/bin/python
from mrjob.job import MRJob
from mrjob.step import MRStep
from nltk import word_tokenize
from nltk.corpus import stopwords
import re

# Uncomment if not installed. If install comment, because nltk.download is bugged
import nltk
# nltk.download('stopwords')
# nltk.download('punkt')

class Top_bigram(MRJob):
    stop_words = set(stopwords.words("english"))
    def mapper_1(self, _, line):
        icp = [x for x in line.strip().split('"') if x != "" and x != " " and x != "\n"]
        if len(icp) == 3:
            phrase = icp[2]
            # stop_words = self.stop_words
            cleaned_line = re.sub(r"[^\w\s]", "", phrase.lower())
            tokenized_str = word_tokenize(cleaned_line)
            words = [
                word
                for word in word_tokenize(cleaned_line)
                if word not in self.stop_words
            ]
            bigrams = [
                f"{words[i]} {words[i+1]}" for i in range(len(words) - 1)
            ]
            for bigram in bigrams:
                yield bigram, 1

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
            MRStep(mapper=self.mapper_1, reducer=self.reducer_1),
            MRStep(reducer=self.reducer_2),
        ]

if __name__ == "__main__":
    Top_bigram().run()
