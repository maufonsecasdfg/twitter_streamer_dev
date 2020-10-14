import os
import sys
import tweepy
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from dotenv import load_dotenv
from textblob import TextBlob
import datetime
import json
import MySQLdb
import time
import random
load_dotenv('./credentials.env')

TWITTER_APP_KEY = os.getenv('TWITTER_APP_KEY')
TWITTER_APP_SECRET = os.environ.get('TWITTER_APP_SECRET')
TWITTER_KEY = os.environ.get('TWITTER_KEY')
TWITTER_SECRET = os.environ.get('TWITTER_SECRET')

class TwitterScraper():
    def __init__(self,db_name,table_names,hashtags,host,user=False,password=False,default_file=False):
        auth = OAuthHandler(TWITTER_APP_KEY,TWITTER_APP_SECRET)
        auth.set_access_token(TWITTER_KEY,TWITTER_SECRET)
        self.api = tweepy.API(auth,wait_on_rate_limit=True,wait_on_rate_limit_notify=True)
        #tables_names = [tweet_table_name, user_table_name, hashtag_table_name, place_table_name, following_table_name]
        if len(table_names) != 5:
            print('tables_names must be passed with the structure [tweet_table_name, user_table_name, hashtag_table_name, place_table_name, following_table_name]')
            sys.exit()
        self.target_hashtags = []
        for ht in hashtags:
            self.target_hashtags.append(ht.replace('#','').lower())
        self.tweet_attributes = ['tweet_id', 'user_id', 'created_at', 'text',
                            'polarity', 'subjectivity', 'in_reply_to_status_id_str',
                            'longitude', 'latitude',
                            'retweet_count', 'favorite_count','lang','place_id']
        self.user_attributes = ['user_id','user_created_at','user_name','user_screen_name',
                                'user_location', 'user_description', 'user_followers_count','user_friends_count']
        self.hashtag_attributes = ['hashtag','tweet_id']
        self.place_attributes = ['place_id','place_type','place_name',
                                'place_full_name','country','country_code']
        self.following_attributes = ['user_id','following_user_id','following_user_name','following_user_screen_name']
        #Open connection with MySQL
        if default_file:
            self.db = MySQLdb.connect(
                            host=host,
                            read_default_file=default_file,
                            db=db_name,
                            use_unicode=True
                        )
        else:
            self.db = MySQLdb.connect(host=host,
                                user=user,
                                passwd=password,
                                db=db_name,
                                use_unicode=True
                        )
        #Create tables in database
        if self.db.open:
            cursor = self.db.cursor()
            tweet_attributes = """
                            tweet_id VARCHAR(255) NOT NULL,
                            user_id VARCHAR(255),
                            created_at VARCHAR(255), 
                            text VARCHAR(300), 
                            polarity REAL, 
                            subjectivity REAL,
                            in_reply_to_status_id_str VARCHAR(255),
                            longitude FLOAT, 
                            latitude FLOAT,
                            retweet_count INT, 
                            favorite_count INT,
                            lang VARCHAR(255),
                            place_id VARCHAR(255),
                            PRIMARY KEY (tweet_id)
                            """
            cursor.execute(f"CREATE TABLE IF NOT EXISTS {table_names[0]} ({tweet_attributes});")

            user_attributes = """
                            user_id VARCHAR(255),
                            user_name VARCHAR(255),
                            user_screen_name VARCHAR(255),
                            user_created_at VARCHAR(255), 
                            user_location VARCHAR(255), 
                            user_description VARCHAR(255), 
                            user_followers_count INT,
                            friends_count INT,
                            PRIMARY KEY (user_id)
                            """
            cursor.execute(f"CREATE TABLE IF NOT EXISTS {table_names[1]} ({user_attributes});")

            hashtag_attributes = """
                            hashtag VARCHAR(255),
                            tweet_id VARCHAR(255),
                            PRIMARY KEY (hashtag,tweet_id)
                            """
            cursor.execute(f"CREATE TABLE IF NOT EXISTS {table_names[2]} ({hashtag_attributes});")

            place_attributes = """
                            place_id VARCHAR(255),
                            place_type VARCHAR(255),
                            place_name VARCHAR(255),
                            place_full_name VARCHAR(255),
                            country VARCHAR(255),
                            country_code VARCHAR(255),
                            PRIMARY KEY (place_id)
                            """
            cursor.execute(f"CREATE TABLE IF NOT EXISTS {table_names[3]} ({place_attributes});")

            following_attributes = """
                            user_id VARCHAR(255),
                            following_user_id VARCHAR(255),
                            following_user_name VARCHAR(255),
                            following_user_screen_name VARCHAR(255),
                            PRIMARY KEY (user_id,following_user_id)
                            """
            cursor.execute(f"CREATE TABLE IF NOT EXISTS {table_names[4]} ({following_attributes});")
            cursor.close()

    def on_data(self,status):
        #status = json.loads(status)
        hashtags = []
        for ht in status['entities']['hashtags']:
            hashtags.append(ht['text'])

        #Extract attributes:
        tweet_id = status['id_str']
        created_at = status['created_at']
        text = status['text']
        if text is not None: text =  text.encode('ascii', 'ignore').decode('ascii').replace('\'', '\\\'').replace('\"', '\\\"') #removes emojis
        sentiment = TextBlob(text)
        polarity = sentiment.polarity
        in_reply_to_status_id_str = status['in_reply_to_status_id_str']
        subjectivity = sentiment.subjectivity
        retweet_count = status['retweet_count']
        favorite_count = status['favorite_count']
        longitude = None
        latitude = None
        if status['coordinates']:
            longitude = status['coordinates']['coordinates'][0]
            latitiude = status['coordinates']['coordinates'][1]
        lang = status['lang']

        user_id = status['user']['id_str']
        user_name = status['user']['name']
        if user_name is not None: user_name = user_name.encode('ascii', 'ignore').decode('ascii').replace('\'', '\\\'').replace('\"', '\\\"')
        user_screen_name = status['user']['screen_name']
        if user_screen_name is not None: user_screen_name = user_screen_name.encode('ascii', 'ignore').decode('ascii').replace('\'', '\\\'').replace('\"', '\\\"')
        user_created_at = status['user']['created_at']
        user_location = status['user']['location']
        if user_location is not None: user_location = user_location.encode('ascii', 'ignore').decode('ascii').replace('\'', '\\\'').replace('\"', '\\\"')
        user_description = status['user']['description']
        if user_description is not None: user_description = user_description.encode('ascii', 'ignore').decode('ascii').replace('\'', '\\\'').replace('\"', '\\\"')
        user_followers_count = status['user']['followers_count']
        user_friends_count = status['user']['friends_count']

        if status['place'] is not None:
            place_id = status['place']['id']
            place_type = status['place']['place_type']
            place_name = status['place']['name']    
            place_full_name = status['place']['full_name']
            country = status['place']['country']
            country_code = status['place']['country_code']
        else:
            place_id = None

        

        #print(tweet_id)

        tweet_values_list = [tweet_id, user_id, created_at, text,
                            polarity, subjectivity, in_reply_to_status_id_str,
                            longitude, latitude,
                            retweet_count, favorite_count,lang,place_id]
        atts = ''
        values = ''
        for i in range(len(self.tweet_attributes)):
            if tweet_values_list[i] is None:
                pass
            else:
                atts += f'{self.tweet_attributes[i]},'
                if isinstance(tweet_values_list[i], str):
                    values += f"'{tweet_values_list[i]}',"
                else:
                    values += f"{tweet_values_list[i]},"
        atts = atts[:-1]
        values = values[:-1]

        if self.db.open:
            cursor = self.db.cursor()
            query = f'INSERT IGNORE INTO tweet_table({atts}) VALUES ({values});'
            cursor.execute(query)
            self.db.commit()
            cursor.close()
        
        user_values_list = [user_id,user_created_at,user_name,user_screen_name,
                            user_location, user_description, user_followers_count, user_friends_count]
        
        atts = ''
        values = ''
        for i in range(len(self.user_attributes)):
            if user_values_list[i] is None:
                pass
            else:
                atts += f'{self.user_attributes[i]},'
                if isinstance(user_values_list[i], str):
                    values += f"'{user_values_list[i]}',"
                else:
                    values += f"{user_values_list[i]},"
        atts = atts[:-1]
        values = values[:-1]

        if self.db.open:
            cursor = self.db.cursor()
            query = f'REPLACE INTO user_table({atts}) VALUES ({values});'
            cursor.execute(query)
            self.db.commit()
            cursor.close()

        for ht in hashtags:
            if self.db.open:
                cursor = self.db.cursor()
                query = f'INSERT IGNORE INTO hashtag_table(hashtag,tweet_id) VALUES (\'{ht}\',\'{tweet_id}\');'
                cursor.execute(query)
                self.db.commit()
                cursor.close()

        if place_id is not None:
            place_values_list = [place_id,place_type,place_name,
                                place_full_name,country,country_code]
            
            atts = ''
            values = ''
            for i in range(len(self.place_attributes)):
                if place_values_list[i] is None:
                    pass
                else:
                    atts += f'{self.place_attributes[i]},'
                    if isinstance(place_values_list[i], str):
                        values += f"'{place_values_list[i]}',"
                    else:
                        values += f"{place_values_list[i]},"
            atts = atts[:-1]
            values = values[:-1]

            if self.db.open:
                cursor = self.db.cursor()
                query = f'INSERT IGNORE INTO place_table({atts}) VALUES ({values});'
                cursor.execute(query)
                self.db.commit()
                cursor.close()

        return True

    def scrape_tweets_from_hashtag(self,date_since,date_until):
        for hashtag in self.target_hashtags:
            print(f'Fetching hashtag: {hashtag}')
            #tweets = tweepy.Cursor(api.search, q=f'#{hashtag}', since=date_since, until=date_until).items()
            tweets = list(tweepy.Cursor(self.api.search, q=f'#{hashtag}').items(1000))
            for tweet in tweets:
                tw = tweet._json
                user = tw['user']
                self.on_data(tw)

    def get_users_verified_follows(self,user_table='user_table',following_table='following_table'):
        user_screen_names = []
        if self.db.open:
            cursor = self.db.cursor()
            cursor.execute(f"SELECT DISTINCT user_id, user_screen_name FROM user_table;")
            users = cursor.fetchall()
        if self.db.open:
            cursor = self.db.cursor()
            cursor.execute(f"SELECT DISTINCT user_id FROM following_table;")
            users_already_scanned = cursor.fetchall()
        users_already_scanned = users_already_scanned[:-1]
        users = list(set(users)-set(users_already_scanned))
        random.shuffle(users)
        print(len(users))
        for user in users:
            user_id = user[0]
            user_screen_name = user[1]
            print(f'Collecting verified followings of user: {user_screen_name}')
            try:
                p = tweepy.Cursor(self.api.friends, screen_name=user_screen_name, count=200).pages()
            except:
                print("Failed to run the command on that user, Skipping...")
                continue
            for page in p:
                for following in page:
                    following = following._json
                    if following['verified']:
                        if self.db.open:
                            cursor = self.db.cursor()
                            following_name = following["name"].replace("'","")
                            following_screen_name = following["screen_name"].replace("'","")
                            print(f'Verified following screen name: {following_screen_name}')
                            query = f'INSERT IGNORE INTO {following_table}(user_id,following_user_id,following_user_name,following_user_screen_name) VALUES (\'{user_id}\',\'{following["id_str"]}\',\'{following_name}\',\'{following_screen_name}\');'
                            cursor.execute(query)
                            self.db.commit()
                            cursor.close()
                time.sleep(60)



if __name__ == '__main__':
    #hashtags = ['#trump','#donaldtrump','#biden','#joebiden','#2020elections','#elections2020']
    hashtags = ['#2020elections','#elections2020']
    table_names = ['tweet_table','user_table','hashtag_table','place_table','following_table']
    date_since = datetime.datetime(2020, 8, 1, 12, 00, 00)
    date_until = datetime.datetime(2020, 8, 31, 12, 00, 00)

    twitter_scraper = TwitterScraper(db_name='TwitterDB',table_names=table_names,hashtags=hashtags,host='localhost',default_file='~/.my.cnf')
    #twitter_scraper.scrape_tweets_from_hashtag(date_since,date_until)
    twitter_scraper.get_users_verified_follows()