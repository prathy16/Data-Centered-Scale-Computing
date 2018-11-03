import boto3
import re

def lambda_handler(event, context):
    # TODO implement
    s3 = boto3.client('s3')
    if event:
        print("Event: ", event)
        file_obj = event["Records"][0]
        filename = str(file_obj['s3']['object']['key'])
        print("filename: ", filename, len(filename))
        fileObj = s3.get_object(Bucket="csci5253-fall2018-project3-gayam-dodmani", Key=filename)
        file_content = fileObj["Body"].read().decode('utf-8')
        print(file_content)
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
        tokens = " ".join(file_content.split())
        for word in tokens.split():
            word = re.sub("[^a-zA-Z']", "", word).lower()
            if len(word) != 0 and word not in stopwords:
                temp2 = re.sub("[']", "", word)
                if len(temp2) != 0 and temp2 not in stopwords:
                    ret_list.append(temp2)
    
        updated_tweet = " ".join(ret_list)
        s3_client = boto3.resource('s3')
        bucket = s3_client.Bucket('output-tweet1-trigger')
        bucket.put_object(Key=filename, Body=updated_tweet)

