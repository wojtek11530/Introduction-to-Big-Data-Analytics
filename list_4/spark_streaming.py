# importing libraries
from collections import namedtuple
import time
import re

import matplotlib.pyplot as plt
import seaborn as sns

import findspark

findspark.init()
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import desc

import pandas as pd


def clean_tweet(tweet):
    return ' '.join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)", " ", str(tweet)).split())


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
    hashtags = lines.flatMap(lambda text: text.split(" ")).filter(lambda text: text.lower().startswith('#'))

    Hashtag = namedtuple('Hashtag', ("tag", "count"))
    Tweet = namedtuple('Tweet', 'text')

    lines.window(40).map(lambda p: Tweet(clean_tweet(p))).foreachRDD(
        lambda rdd: rdd.toDF().registerTempTable("tweets"))

    hashtags.window(40) \
        .map(lambda word: (word, 1)) \
        .reduceByKey(lambda a, b: a + b) \
        .map(lambda p: Hashtag(p[0], p[1])) \
        .foreachRDD(lambda rdd: rdd.toDF().registerTempTable("hashtags"))

    time_to_wait = 80
    ssc.start()
    print("Session Started.....")
    print("Collecting tweets...waiting for " + str(time_to_wait) + " seconds..")
    time.sleep(time_to_wait)
    print("Tweets Collected....")

    top_10_from_all = None
    all_tweets_df = None

    count = 1
    count_max = 1
    while count <= count_max:
        print('Count: ' + str(count) + "/" + str(count_max))
        print("Waiting for 30 Seconds.....")
        time.sleep(30)  # This loop will run every 30 seconds. The time interval can be increased as per your wish

        top_10_tags = sqlContext.sql('Select tag, count from hashtags')
        top_10_df = top_10_tags.toPandas()
        print(top_10_df)

        if top_10_from_all is None:
            top_10_from_all = top_10_df
        else:
            top_10_from_all = pd.concat([top_10_from_all, top_10_df], join='inner', ignore_index=True)

        tweets = sqlContext.sql('Select text from tweets')
        tweets_df = tweets.toPandas()
        print(tweets_df)

        if all_tweets_df is None:
            all_tweets_df = tweets_df
        else:
            all_tweets_df = pd.concat([all_tweets_df, tweets_df], join='inner', ignore_index=True)

        count += 1

    ssc.stop()

    print('#' * 25)
    print('Grouped\n')
    grouped = top_10_from_all.groupby('tag').sum().reset_index()
    grouped_sorted_df = grouped.sort_values('count', ascending=False)
    print(grouped_sorted_df)

    if top_10_from_all is not None:
        top_10_from_all.to_csv('hashtags.csv')
    if all_tweets_df is not None:
        all_tweets_df.to_csv('tweets.csv')

    # ssc.stop(stopSparkContext=True, stopGraceFully=True)


if __name__ == '__main__':
    run()
