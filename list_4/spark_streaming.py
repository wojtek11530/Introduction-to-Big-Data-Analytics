# importing libraries
from collections import namedtuple
import time
import re
import string


from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
from nltk.sentiment.vader import SentimentIntensityAnalyzer


import findspark
findspark.init()
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SQLContext


import pandas as pd


def clean_tweet(tweet):
    return ' '.join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)", " ", str(tweet)).split())


def analyze_sentiment_polarity(tweet):
    analyzer = SentimentIntensityAnalyzer()
    sentiment = analyzer.polarity_scores(tweet)
    if sentiment['compound'] >= 0.05:
        return "Positive"
    elif sentiment['compound'] <= - 0.05:
        return "Negative"
    else:
        return "Neutral"


def run():
    # Creating the Spark Context
    sc = SparkContext(master="local[2]", appName="WindowWordCount")
    sc.setLogLevel("ERROR")

    # creating the streaming context
    ssc = StreamingContext(sc, 10)
    ssc.checkpoint("checkpoint")

    # creating the SQL context
    sqlContext = SQLContext(sc)

    host = "localhost"
    port = 5599

    lines = ssc.socketTextStream(host, port)

    hashtags = lines.filter(lambda text: len(text) > 0) \
        .flatMap(lambda text: text.split(" ")) \
        .filter(lambda text: text.lower().startswith('#'))

    Word = namedtuple('Word', ("word", "count"))
    Hashtag = namedtuple('Hashtag', ("tag", "count"))
    Tweet = namedtuple('Tweet', ('text', 'sentiment'))

    stop_words = set(stopwords.words('english'))
    list_punct = list(string.punctuation)
    lemmatizer = WordNetLemmatizer()

    lines.window(40) \
        .map(lambda p: clean_tweet(p)) \
        .filter(lambda text: len(text) > 0) \
        .map(lambda p: Tweet(p, analyze_sentiment_polarity(p))) \
        .foreachRDD(lambda rdd: rdd.toDF().registerTempTable("tweets"))

    lines.window(40) \
        .map(lambda p: clean_tweet(p)) \
        .filter(lambda text: len(text) > 0) \
        .flatMap(lambda text: text.split(" ")) \
        .map(lambda word: word.lower()) \
        .filter(lambda word: word not in stop_words) \
        .map(lambda word: ''.join(char for char in word if char not in list_punct)) \
        .map(lambda word: lemmatizer.lemmatize(word)) \
        .map(lambda word: (word, 1)) \
        .reduceByKey(lambda a, b: a + b) \
        .map(lambda p: Word(p[0], p[1])) \
        .foreachRDD(lambda rdd: rdd.toDF().registerTempTable("words"))

    hashtags.window(40) \
        .map(lambda word: ''.join(char for char in word if char not in list_punct)) \
        .map(lambda word: (word.lower(), 1)) \
        .reduceByKey(lambda a, b: a + b) \
        .map(lambda p: Hashtag(p[0], p[1])) \
        .foreachRDD(lambda rdd: rdd.toDF().registerTempTable("hashtags"))

    time_to_wait = 80
    ssc.start()
    print("Session Started.....")
    print("Collecting tweets...waiting for " + str(time_to_wait) + " seconds..")
    time.sleep(time_to_wait)
    print("Tweets Collected....")

    all_hashtags_df = None
    all_tweets_df = None
    all_words_df = None

    count = 1
    count_max = 4
    while count <= count_max:
        print('Count: ' + str(count) + "/" + str(count_max))
        print("Waiting for 30 Seconds.....")
        time.sleep(40)  # This loop will run every 30 seconds. The time interval can be increased as per your wish

        words = sqlContext.sql('Select word, count from words')
        words_df = words.toPandas()
        print(words_df)
        if all_words_df is None:
            all_words_df = words_df
        else:
            all_words_df = pd.concat([all_words_df, words_df], join='inner', ignore_index=True)

        tags = sqlContext.sql('Select tag, count from hashtags')
        tags_df = tags.toPandas()
        print(tags_df)
        if all_hashtags_df is None:
            all_hashtags_df = tags_df
        else:
            all_hashtags_df = pd.concat([all_hashtags_df, tags_df], join='inner', ignore_index=True)

        tweets = sqlContext.sql('Select text, sentiment from tweets')
        tweets_df = tweets.toPandas()
        if all_tweets_df is None:
            all_tweets_df = tweets_df
        else:
            all_tweets_df = pd.concat([all_tweets_df, tweets_df], join='inner', ignore_index=True)

        count += 1

    ssc.stop()

    if all_hashtags_df is not None:
        all_hashtags_df.to_csv('hashtags.csv')
    if all_words_df is not None:
        all_words_df.to_csv('words.csv')
    if all_tweets_df is not None:
        all_tweets_df.to_csv('tweets.csv')




if __name__ == '__main__':
    run()
