import re
import sys
from operator import add
from pyspark import SparkConf, SparkContext

conf = (SparkConf().setMaster("local").setAppName("WordCounter").set("spark.executor.memory", "1g"))
sc = SparkContext(conf=conf)

if __name__ == "__main__":
    stopwords = ["a","about","above","after" ,"again","against", "all", "am", "an", "and", "any", "are",
                 "aren't", "as", "at", "be", "because", "been", "before", "being", "below", "between", "both",
                 "but", "by", "can't", "cannot", "could", "couldn't", "did", "didn't", "do", "does", "doesn't",
                 "doing", "don't", "down", "during", "each", "few", "for", "from", "further", "had", "hadn't",
                 "has", "hasn't", "have", "haven't", "having", "he", "he'd", "he'll", "he's", "her", "here", "here's",
                 "hers", "herself", "him", "himself", "his", "how", "how's", "i", "i'd", "i'll", "i'm",
                 "i've" ,"if" ,"in" ,"into" ,"is" ,"isn't" ,"it" ,"it's" ,"its" ,"itself" ,"let's" ,"me" ,
                 "more" ,"most" ,"mustn't" ,"my" ,"myself" ,"no" ,"nor" ,"not" ,"of" ,"off" ,"on" ,"once" ,
                 "only" ,"or" ,"other" ,"ought" ,"our" ,"ours" ,"ourselves" ,"out" ,"over" ,"own" ,"same" ,
                 "shan't" ,"she" ,"she'd" ,"she'll" ,"she's" ,"should" ,"shouldn't" ,"so" ,"some" ,"such" ,"than" ,
                 "that" ,"that's" ,"the" ,"their" ,"theirs" ,"them" ,"themselves" ,"then" ,"there" ,"there's" ,"these" ,
                 "they" ,"they'd" ,"they'll" ,"they're" ,"they've" ,"this" ,"those" ,"through" ,"to" ,"too" ,"under" ,"until" ,
                 "up" ,"very" ,"was" ,"wasn't" ,"we" ,"we'd" ,"we'll" ,"we're" ,"we've" ,"were" ,"weren't" ,"what" ,
                 "what's" ,"when" ,"when's" ,"where" ,"where's" ,"which" ,"while" ,"who" ,"who's" ,"whom" ,"why" ,"why's" ,
                 "with" ,"won't" ,"would" ,"wouldn't" ,"you" ,"you'd" ,"you'll" ,"you're" ,"you've" ,"your" ,"yours" ,
                 "yourself" ,"yourselves"]


    lines = sc.textFile(sys.argv[1])

    lines_nonempty = lines.filter( lambda x: len(x) > 0)
    counts = lines_nonempty.flatMap(lambda x: x.split()).map(lambda x: (x, 1)).reduceByKey(add)
    output = counts.collect()
    output_dict = dict()
    for (word, count) in output:
        temp1 = re.sub("[^a-zA-Z']", "", word).lower()
        if len(temp1)!=0 and temp1 not in stopwords:
            temp2 = re.sub("[']", "", temp1)
            if temp2 != "" and temp2 not in stopwords:
                if temp2 not in output_dict.keys():
                    output_dict[temp2] = 0
                output_dict[temp2] += count
    word_sorted_count = sorted(output_dict, key=lambda x: (-output_dict[x],x))

    counter = 0
    for k in word_sorted_count:
        if counter < 2000:
            print("{}\t{}".format(k, output_dict[k]))
            counter += 1
    sc.stop()