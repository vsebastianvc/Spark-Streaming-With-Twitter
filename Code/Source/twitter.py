# -*- coding: utf-8 -*-
import json
import re
import time
import uuid
from kafka import KafkaProducer
import json
import tweepy
from textblob import TextBlob

#Credentials of the Twitter API
consumer_key = 'XqS57zAjgQfkjnCnVeOP5g'
consumer_secret = 'dYrsaxWfncWnNVPnjaLNrxZdihxJXjzkBOtRbO3UY6A'
access_token = '17996901-LwSSjZXS9YPZOy7quSL7hwd1w0OfMp0rHWVTcZXOO'
access_token_secret = 'JINn7zsyQ5L0BiQ9wNcSoKp0wfiqRwgJPqLDyP5xpjUxN'

#
SECONDS=60
KAFKA_SERVER='localhost:9092'
TWITTER_MAX_NUM_HASHTAG=5
KAFKA_TOPIC="projectTweets"
TIME_TO_SLEEP_BY_NEXT_TWEETS=SECONDS*10
TIME_TO_SLEEP_BY_NEXT_TWEETS_ERROR=SECONDS*30

auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
api = tweepy.API(auth, wait_on_rate_limit=True)
kafka_producer = KafkaProducer(value_serializer=lambda v: json.dumps(v).encode('utf-8'),bootstrap_servers=KAFKA_SERVER)

#Method that get the tweets using an array of specific topics
def crawler():
    for socialmedia in ['#facebook','#youtube','#twitter','#instragram','#linkedin']:
        dataset = get_tweets(query=socialmedia, count=TWITTER_MAX_NUM_HASHTAG)
        for element in dataset:
            element["company"] = socialmedia
            info=kafka_producer.send(KAFKA_TOPIC, element)
            print ("Status data ",info)
    return True

#Method that return tweets
def get_tweets(query="#facebook", count=100):
    #Search the tweets with a especific query
    dataset = api.search(q=query, count=count)
    tweets = []
    #for each tweet get the text
    for tweet in dataset:
        sub_set_tweet = {}
        sub_set_tweet['text'] = tweet.text
        #if the tweet has retweet 
        if tweet.retweet_count > 0:
            #if this retweet does not exist in the array then add
            if sub_set_tweet not in tweets:
                tweets.append(sub_set_tweet)
        else:
            tweets.append(sub_set_tweet)
    return tweets

def main():
    while True:
        try:
            print("Trying to get the last tweets about socialmedia please wait :) ....")
            crawler()
            print("Sleep time to get new tweets")
            time.sleep(TIME_TO_SLEEP_BY_NEXT_TWEETS)
        except:

            print("Error , Something happend please wait few seconds....")
            time.sleep(TIME_TO_SLEEP_BY_NEXT_TWEETS_ERROR)


main()
