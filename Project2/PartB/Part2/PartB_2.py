from pyspark.sql import SparkSession
import pyspark.sql.functions as func
from pyspark.sql.functions import udf
import re

my_spark = SparkSession.builder.getOrCreate()
df = my_spark.read.format("com.mongodb.spark.sql.DefaultSource").option("uri", "mongodb://student:student@ec2-54-210-44-189.compute-1.amazonaws.com/test.reviews").load()

def normalizeString(string):
    stopwords = ["a", "about", "above", "after", "again", "against", "all", "am", "an", "and", "any", "are",
                 "aren't", "as", "at", "be", "because", "been", "before", "being", "below", "between", "both",
                 "but", "by", "can't", "cannot", "could", "couldn't", "did", "didn't", "do", "does", "doesn't",
                 "doing", "don't", "down", "during", "each", "few", "for", "from", "further", "had", "hadn't",
                 "has", "hasn't", "have", "haven't", "having", "he", "he'd", "he'll", "he's", "her", "here", "here's",
                 "hers", "herself", "him", "himself", "his", "how", "how's", "i", "i'd", "i'll", "i'm",
                 "i've", "if", "in", "into", "is", "isn't", "it", "it's", "its", "itself", "let's", "me",
                 "more", "most", "mustn't", "my", "myself", "no", "nor", "not", "of", "off", "on", "once",
                 "only", "or", "other", "ought", "our", "ours", "ourselves", "out", "over", "own", "same",
                 "shan't", "she", "she'd", "she'll", "she's", "should", "shouldn't", "so", "some", "such", "than",
                 "that", "that's", "the", "their", "theirs", "them", "themselves", "then", "there", "there's", "these",
                 "they", "they'd", "they'll", "they're", "they've", "this", "those", "through", "to", "too", "under",
                 "until",
                 "up", "very", "was", "wasn't", "we", "we'd", "we'll", "we're", "we've", "were", "weren't", "what",
                 "what's", "when", "when's", "where", "where's", "which", "while", "who", "who's", "whom", "why",
                 "why's",
                 "with", "won't", "would", "wouldn't", "you", "you'd", "you'll", "you're", "you've", "your", "yours",
                 "yourself", "yourselves"]

    ret_list = []
    tokens = " ".join(string.split())
    for word in tokens.split():
        word = re.sub("[^a-zA-Z']", "", word).lower()
        if len(word) != 0 and word not in stopwords:
            temp2 = re.sub("[']", "", word)
            if len(temp2) != 0 and temp2 not in stopwords:
                ret_list.append(temp2)

    return " ".join(ret_list)


review_tokens = udf(normalizeString)

for i in range(1, 6):
    reviewText_rating = df.filter(df["overall"] == i * 1.0).select(["reviewText"])
    reviewText_rating = reviewText_rating.withColumn("reviewTextString", review_tokens(df["reviewText"]))
    reviewText_rating = reviewText_rating.drop("reviewText")
    review_tokens = reviewText_rating.withColumn("reviewTextWords", func.explode(func.split("reviewTextString", "\s+")))
    review_tokens = review_tokens.drop("reviewTextString")
    tokens_count = review_tokens.groupBy("reviewTextWords").agg(func.count(review_tokens["reviewTextWords"])).alias("count")
    output = tokens_count.rdd.takeOrdered(500, lambda x: (-x[1], x[0]))
    with open("Bucket%d/output.txt" % i, "w") as file:
        for line in output:
            file.write("%s\t%i\n" % (line[0], line[1]))
