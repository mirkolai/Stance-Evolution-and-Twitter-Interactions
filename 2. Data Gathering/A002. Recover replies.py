__author__ = 'mirko'

import pymysql
import oauth2 as oauth
import time
import json
import config as cfg
from datetime import datetime
from dateutil import tz
import sys


db = pymysql.connect(host=cfg.mysql['host'], # your host, usually localhost
             user=cfg.mysql['user'], # your username
             passwd=cfg.mysql['passwd'], # your password
             db=cfg.mysql['db'],
             charset='utf8mb4',
             use_unicode=True) # name of the data base

cur = db.cursor()
cur.execute('SET NAMES utf8mb4')
cur.execute("SET CHARACTER SET utf8mb4")
cur.execute("SET character_set_connection=utf8mb4")
db.commit()

CONSUMER_KEY    = cfg.twitter["CONSUMER_KEY"]
CONSUMER_SECRET = cfg.twitter["CONSUMER_SECRET"]
ACCESS_KEY      = cfg.twitter["ACCESS_KEY"]
ACCESS_SECRET   = cfg.twitter["ACCESS_SECRET"]


consumer = oauth.Consumer(key=CONSUMER_KEY, secret=CONSUMER_SECRET)
access_token = oauth.Token(key=ACCESS_KEY, secret=ACCESS_SECRET)
client = oauth.Client(consumer, access_token)


"""
Given a list of ids tweets, it store replied tweets in the DB and it adds new ids to tweet_ids if new replies are recovered.
"""
def return_reply(tweet_ids):

    while 1:
        try:
            #print("asked: ",len(tweet_ids))
            parameter = ','.join(tweet_ids[0:99]) #max 100 id per request
            place_endpoint ="https://api.twitter.com/1.1/statuses/lookup.json?id="+parameter
            response, data = client.request(place_endpoint)
            if response['status']=='200':
                if int(response['x-rate-limit-remaining'])<2:
                    print('id rescue: wait '+str( int(response['x-rate-limit-reset']) - int(time.time()) )+' seconds')
                    time.sleep(int(response['x-rate-limit-reset'])-int(time.time()))

            jsonTweet=json.loads(data.decode("utf-8"))
            #print("found: ",str(len(jsonTweet)))

            print('id rescue: wait '+str((15*60)/int(response['x-rate-limit-limit']))+' seconds')
            time.sleep((15*60)/int(response['x-rate-limit-limit']))

            return(jsonTweet)

        except:
            e = sys.exc_info()[0]
            print('exeption '+str(e))
            time.sleep(60)





if __name__ == "__main__":


    """
    Here, the script recursively recover replies.
    Fist, it recover id for each reply present in table `tweet`.
    Then, it verify if the replied tweet r is itself a reply.
    If r is a reply, its replied tweet r-1 will be recovered.

    In this script you need a new table. The table `tweet_reply`.

    """
    cur.execute("""
    CREATE TABLE IF NOT EXISTS `tweet_reply` (
      `id` bigint(20) NOT NULL,
      `user_id` bigint(20) NOT NULL,
      `screen_name` varchar(100) COLLATE utf8mb4_unicode_ci NOT NULL,
      `text` varchar(2500) COLLATE utf8mb4_unicode_ci NOT NULL,
      `date` datetime NOT NULL,
      `retweet` int(11) NOT NULL,
      `reply` bigint(20) NOT NULL,
      `quote` bigint(20) NOT NULL,
      `json` text COLLATE utf8mb4_unicode_ci NOT NULL,
      PRIMARY KEY (`id`),
      KEY `user_id` (`user_id`,`screen_name`)
    ) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
    """)
    db.commit()

    cur.execute("truncate table `tweet_reply`")
    db.commit()


    cur.execute(" SELECT distinct(`reply`) FROM `tweet` WHERE `reply` <> 0 and reply not in (select id from tweet_reply ) ")

    tweets=cur.fetchall()
    tweet_ids=[]

    for tweet in tweets:
        tweet_ids.append(str(tweet[0]))

    while len(tweet_ids) > 0:
        print("remain: "+str(len(tweet_ids)))
        print("remain - 99: "+str(len(tweet_ids)-99))

        jsonTweets=return_reply(tweet_ids[0:99])
        tweet_ids = tweet_ids[99:]

        for jsonTweet in jsonTweets:

            quote=0
            if jsonTweet.get('quoted_status'):
                if jsonTweet['quoted_status']['id'] not in tweet_ids:
                    quote=jsonTweet['quoted_status']['id']

            if jsonTweet['in_reply_to_status_id'] is not None:
                if jsonTweet['in_reply_to_status_id'] not in tweet_ids:
                    tweet_ids.append(str(jsonTweet['in_reply_to_status_id']))

            if not jsonTweet.get('retweeted_status'):
                retweet=0
                text=jsonTweet['text']
            else:
                retweet=jsonTweet['retweeted_status']['user']['id']
                text=jsonTweet['retweeted_status']['text']

            if jsonTweet['in_reply_to_status_id'] is None:
                reply=0
            else:
                reply=jsonTweet['in_reply_to_status_id']

            date=datetime.strptime(jsonTweet['created_at'],'%a %b %d %H:%M:%S +0000 %Y')\
                .replace(tzinfo=tz.gettz('UTC'))\
                .astimezone(tz.gettz('Europe/Rome'))

            cur.execute(" INSERT INTO `tweet_reply`"
                        " (`id`, `user_id`, `screen_name`, `text`, `date`, `retweet`, `reply`, quote, `json`) "
                        " VALUES "
                        "( %s, %s, %s, %s, %s, %s, %s, %s,%s) on duplicate key update id=id",
                        (jsonTweet['id'],jsonTweet['user']['id'],jsonTweet['user']['screen_name'],jsonTweet['text'],date, retweet,reply, quote, json.dumps(jsonTweet)))




