
# nltk.download('stopwords')
# nltk.download('punkt')

from mrjob.job import MRJob
from mrjob.step import MRStep
# from nltk.corpus import stopwords # импорт проходит успешна, а загрузка стоп-слов что бы я не пробовал не проходит
from nltk import word_tokenize # импорт проходит успешна, а загрузка стоп-слов что бы я не пробовал не проходит
import re
from nltk.stem.snowball import SnowballStemmer

class Top_bigram(MRJob):
    stopwords = set(stopwords.words("english"))
    # stemmer=SnowballStemmer("english")
    def mapper_1_init(self):
        # stopwords = open("/data/stopwords/corpora/stopwords/english").read() # не работает на хадупе ни в какую...
        self.stop_words = set(stopwords.words("english")) # не работает на хадупе ни в какую...
        # self.stopwords = ["i","me","my","myself","we","our","ours","ourselves","you","you're","you've","you'll","you'd","your","yours","yourself","yourselves","he","him","his","himself","she","she's","her","hers","herself","it","it's","its","itself","they","them","their","theirs","themselves","what","which","who","whom","this","that","that'll","these","those","am","is","are","was","were","be","been","being","have","has","had","having","do","does","did","doing","a","an","the","and","but","if","or","because","as","until","while","of","at","by","for","with","about","against","between","into","through","during","before","after","above","below","to","from","up","down","in","out","on","off","over","under","again","further","then","once","here","there","when","where","why","how","all","any","both","each","few","more","most","other","some","such","no","nor","not","only","own","same","so","than","too","very","s","t","can","will","just","don","don't","should","should've","now","d","ll","m","o","re","ve","y","ain","aren","aren't","couldn","couldn't","didn","didn't","doesn","doesn't","hadn","hadn't","hasn","hasn't","haven","haven't","isn","isn't","ma","mightn","mightn't","mustn","mustn't","needn","needn't","shan","shan't","shouldn","shouldn't","wasn","wasn't","weren","weren't","won","won't","wouldn","wouldn't",]
        self.stemmer = SnowballStemmer("english")
        self.tokenizer = word_tokenize
        pass
    def mapper_1(self, _, line):
        icp = [x for x in line.split('"') if x != "" and x != " "]
        if len(icp) == 3:
            # try:
                phrase = icp[2]
                cleaned_line = re.sub(r"[^\w\s]", "", phrase.lower())
                words = []
                for word in self.tokenizer(cleaned_line):
                    flag = True
                    for x in self.stop_words:
                        if word == x:
                            flag=False
                            break
                    if flag:
                        words.append(self.stemmer.stem(word))
                # words = [
                    # self.stemmer.stem(word)
                    # for word in word_tokenize(cleaned_line)
                    # if word not in self.stop_words
                # ]
                bigrams = [
                    f"{words[i]} {words[i+1]}" for i in range(len(words) - 1)
                ]
                for bigram in bigrams:
                    yield bigram, 1
            # except:
            #     pass
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
    Top_bigram().run()
