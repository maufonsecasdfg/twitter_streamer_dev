import MySQLdb

if __name__ == '__main__':

    db = MySQLdb.connect(
                host='localhost',
                read_default_file='~/.my.cnf',
                db='TwitterDB',
                use_unicode=True
                )

    table_names = ['tweet_table','user_table','hashtag_table','place_table']

    if db.open:
        cursor = db.cursor()
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
        cursor.close() 