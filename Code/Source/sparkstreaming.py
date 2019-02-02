import os
import re
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from textblob import TextBlob
import hbasetable

#Method that filter and clean the tweet to be analyze
def filter_clean_tweet(tweet):
    #Using a lambda expression delete the special characters and split the words of the tweet
    try:
        fx = lambda x: ' '.join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])| (\w+:\ / \ / \S+)", " ", x).split())
        return fx(tweet)
    except:
        pass
    return " NULL "

#Method that analyze a passed tweet text
def sentiment_analysis(tweet):
    #Create TextBlob object of passed tweet text
    analysis = TextBlob(filter_clean_tweet(tweet))
    #According the polarity choose if the tweet is positive,negative or neutral
    if analysis.sentiment.polarity > 0:
        return 'positive'
    elif analysis.sentiment.polarity == 0:
        return 'neutral'
    else:
        return 'negative'

#Build the tweet with the setiment analysis and insert them
def rebuild(xrow):
    class Struct:
        def __init__(self, **entries):
            self.__dict__.update(entries)

        def __str__(self):
            return self.__dict__.values()

    rows = xrow.collect()
    index=0
    #for each key pair in rows add the row into the table socialmedia in the hbase
    for key_pair in rows:
        index=index+1
        key, value = key_pair
        if value:
            try:

                value = Struct(**eval(value))
                solution = (value.company, value.text, 1)
                company, analized, quantity = solution
                row = {b'family:company': bytes(company, 'utf-8'),
                       b'family:text': bytes(filter_clean_tweet(value.text), 'utf-8'),
                       b'family:analized': bytes(sentiment_analysis(analized), 'utf-8'),
                       b'family:quantity': bytes("{quantity}".format(quantity=quantity), 'utf-8')}
                hbasetable.write_row(row)
                print("Inserted Row {index} Completed".format(index=index))

            except Exception as e:
                print("Error  {error} in index {index}".format(index=index,error=str(e)))
    print("Waiting for Tweets... ")

    return xrow


def main():
    #hbase.create_table(delete_table=True)
    print ("Initializing...")
    print ("Please wait...")
    os.environ['PYSPARK_SUBMIT_ARGS'] = '--jars /home/cloudera/Desktop/BigDataTechnologiesProject/spark-streaming-kafka.jar pyspark-shell'
    #Create a local SparkContext with two working thread
    sparkContext = SparkContext("local[2]", "TweetsStreamConsumer")
    #Create a local StreamingContext Batch interval of 3 second
    sparkStreamingContext = StreamingContext(sparkContext, 3)
    kafkaStream = KafkaUtils.createDirectStream(sparkStreamingContext, ['projectTweets'], {'metadata.broker.list': 'localhost:9092'})
    parsed = kafkaStream.map(lambda v: v)
    parsed.foreachRDD(lambda t, rdd: rebuild(rdd))

    print("Starting the task process...")
    sparkStreamingContext.start()
    sparkStreamingContext.awaitTermination()



main()
