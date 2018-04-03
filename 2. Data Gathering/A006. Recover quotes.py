
__author__ = 'mirko'
from dateutil import tz
import os
import pymysql
import oauth2 as oauth
import time
import json
import config as cfg
from datetime import datetime
import sys

def return_reply(tweet_ids):
    CONSUMER_KEY    = cfg.twitter["CONSUMER_KEY"]
    CONSUMER_SECRET = cfg.twitter["CONSUMER_SECRET"]
    ACCESS_KEY      = cfg.twitter["ACCESS_KEY"]
    ACCESS_SECRET   = cfg.twitter["ACCESS_SECRET"]


    consumer = oauth.Consumer(key=CONSUMER_KEY, secret=CONSUMER_SECRET)
    access_token = oauth.Token(key=ACCESS_KEY, secret=ACCESS_SECRET)
    client = oauth.Client(consumer, access_token)
    while 1:
        try:
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







"""
If the tweet is a quote, it recovers the original retweet.
If the quoted tweet is a reply, it also recursively recovers the replied tweet.
"""
print(os.getcwd())

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


cur.execute("""CREATE TABLE IF NOT EXISTS `tweet_quote` (
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

cur.execute(" SELECT distinct(`quote`) FROM `tweet` WHERE `quote` <> 0 and quote not in (select id from tweet_quote ) "
            " union "
            " SELECT distinct(`quote`) FROM `tweet_retweet` WHERE `quote` <> 0 and quote not in (select id from tweet_quote ) "
            " union "
            " SELECT distinct(`quote`) FROM `tweet_reply` WHERE `quote` <> 0 and quote not in (select id from tweet_quote ) "
            " union "
            " SELECT distinct(`quote`) FROM `tweet_quote` WHERE `quote` <> 0 and quote not in (select id from tweet_quote ) "
            " union "
            " SELECT distinct(`quote`) FROM `tweet_retweet_original` WHERE `quote` <> 0 and quote not in (select id from tweet_quote ) "
            )

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
                tweet_ids.append(str(quote))

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


        cur.execute(" INSERT INTO `tweet_quote`"
                    " (`id`, `user_id`, `screen_name`, `text`, `date`, `retweet`, `reply`, quote, `json`) "
                    " VALUES "
                    "( %s, %s, %s, %s, %s, %s, %s, %s,%s) on duplicate key update id=id",
                    (jsonTweet['id'],jsonTweet['user']['id'],jsonTweet['user']['screen_name'],jsonTweet['text'],date, retweet,reply,quote, json.dumps(jsonTweet)))




