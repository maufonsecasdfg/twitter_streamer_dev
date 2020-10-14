from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from kafka import KafkaProducer, KafkaClient
from dotenv import load_dotenv
import os
import json

load_dotenv('./credentials.env')

TWITTER_APP_KEY = os.getenv('TWITTER_APP_KEY')
TWITTER_APP_SECRET = os.environ.get('TWITTER_APP_SECRET')
TWITTER_KEY = os.environ.get('TWITTER_KEY')
TWITTER_SECRET = os.environ.get('TWITTER_SECRET')

class TwitterStreamer():
    def stream_tweets(self,hashtags,kafka_topic):
        listener = TwitterListener(kafka_topic)
        auth = OAuthHandler(TWITTER_APP_KEY,TWITTER_APP_SECRET)
        auth.set_access_token(TWITTER_KEY,TWITTER_SECRET)

        stream = Stream(auth,listener,verify = False)

        stream.filter(track=hashtags)

class TwitterListener(StreamListener):
    def __init__(self,kafka_topic):
        self.kafka_topic = kafka_topic
        self.kf_prod = KafkaProducer(bootstrap_servers='localhost:9092',value_serializer=lambda m: json.dumps(m).encode('utf-8'))
    def on_data(self, data):
        self.kf_prod.send(self.kafka_topic, data)
        #print (data)
        return True
    def on_error(self, status):
        print (status)


if __name__ == '__main__':
    hashtags = ['#trump']

    twitter_streamer = TwitterStreamer()
    twitter_streamer.stream_tweets(hashtags=hashtags,kafka_topic='twitter_trump_test')
