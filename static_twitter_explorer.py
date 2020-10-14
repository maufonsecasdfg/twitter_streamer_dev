import MySQLdb
import datetime
import pandas as pd

if __name__ == '__main__':

        db = MySQLdb.connect(
                host='localhost',
                read_default_file='~/.my.cnf',
                db='TwitterDB',
                use_unicode=True
                )

    #Get tweets from past 10 mins
#     time_now = datetime.datetime.utcnow()
#     time_10mins_before = datetime.timedelta(hours=0,minutes=10)
#     time_interval = (time_now - time_10mins_before).strftime('%Y-%m-%d %H:%M:%S')

        query = """SELECT tweet_id, text, created_at, polarity, user_name
            FROM tweet_table
            LEFT JOIN user_table
            ON tweet_table.user_id = user_table.user_id;"""
            #WHERE created_at >= 'time_interval';
        query = """SELECT *
            FROM hashtag_table
            """
            #WHERE created_at >= 'time_interval';
            

        df = pd.read_sql(query, con=db)

        #df['created_at'] = pd.to_datetime(df['created_at'])

        print(df)
