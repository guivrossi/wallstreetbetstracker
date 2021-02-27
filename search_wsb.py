from re import sub
import config
import pandas as pd 
from psaw import PushshiftAPI
import datetime as dt
import psycopg2
import psycopg2.extras

connection = psycopg2.connect(host=config.DB_HOST, database=config.DB_NAME, user=config.DB_USER, password=config.DB_PASS)
cursor = connection.cursor(cursor_factory=psycopg2.extras.DictCursor)

cursor.execute("""
    SELECT * FROM stock
""")

rows = cursor.fetchall()

stocks = {}

for row in rows:
    stocks['$' + row['symbol']] = row['id']


api = PushshiftAPI()
start_time = int(dt.datetime(2021, 1 , 30).timestamp())

submissions = api.search_submissions(after=start_time,
                                     subreddit = 'wallstreetbets',
                                     filter=['url','autor','title','subreddit']
                                     )

for submission in submissions:
    # print('Title: {}'.format(submission.subreddit))
    # print(submission.created_utc)
    # print(submission.title)
    # print(submission.url)

    words = submission.title.split()
    cashtags = list(set(filter(lambda word: word.lower().startswith("$"), words)))
    
    if len(cashtags) > 0:
        # print(cashtags)
        # print('Title: {}'.format(submission.title))
        # print(submission.created_utc)
        
        for cashtag in cashtags:
            
            submitted_time = dt.datetime.fromtimestamp(submission.created_utc).isoformat()
            submitted_date = dt.date.fromtimestamp(submission.created_utc).isoformat()

            try:
                cursor.execute(
                    """
                    INSERT INTO mention (dt,date, stock_id, message, source, url)
                    VALUES (%s, %s, %s, %s,'walstreetbets', %s )
                    """, (submitted_time, submitted_date, stocks[cashtag], submission.title, submission.url)
                )

                connection.commit()
            except Exception as e:
                print(e)
                connection.rollback()
