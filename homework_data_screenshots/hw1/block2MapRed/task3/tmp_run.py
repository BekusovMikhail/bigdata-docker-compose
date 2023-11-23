import os
import re
from nltk import word_tokenize
from nltk.corpus import stopwords
data = "../SW_EpisodeIV.txt"
print(os.listdir())
stop_words = stopwords.words("english")
for line in open(data):
    icp = [x for x in line.split('"') if x != "" and x != " " and x!="\n"]
    print(icp)
    if len(icp) == 3:
        phrase = icp[2]
        # stop_words = self.stop_words
        cleaned_line = re.sub(r"[^\w\s]", "", phrase.lower())
        words = [
            word
            for word in word_tokenize(cleaned_line)
            if word not in stop_words
        ]
        bigrams = [
            f"{words[i]} {words[i+1]}" for i in range(len(words) - 1)
        ]
        for bigram in bigrams:
            print(bigram)
