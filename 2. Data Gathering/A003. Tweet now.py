
__author__ = 'mirko'
import oauth2 as oauth
import time
import json
import config as cfg
import pymysql
import sys
from datetime import datetime
from dateutil import tz

CONSUMER_KEY    = cfg.twitter["CONSUMER_KEY"]
CONSUMER_SECRET = cfg.twitter["CONSUMER_SECRET"]
ACCESS_KEY      = cfg.twitter["ACCESS_KEY"]
ACCESS_SECRET   = cfg.twitter["ACCESS_SECRET"]


consumer = oauth.Consumer(key=CONSUMER_KEY, secret=CONSUMER_SECRET)
access_token = oauth.Token(key=ACCESS_KEY, secret=ACCESS_SECRET)
client = oauth.Client(consumer, access_token)

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

"""
This script recovers the original tweet for each retweet in table `tweet`.
The tweet is stored in the table `tweet_retweet_original`.

Then, it store the current JSON of each tweet
(to know the current value of retweet_count is usefully for recovering all retweets for each tweet in the next stage.)
The tweet i stored in the tablee `tweet_now`.

"""


cur.execute("""CREATE TABLE IF NOT EXISTS `tweet_retweet_original` (
  `id` bigint(20) NOT NULL,
  `user_id` bigint(20) NOT NULL,
  `screen_name` varchar(100) COLLATE utf8mb4_unicode_ci NOT NULL,
  `text` varchar(2500) COLLATE utf8mb4_unicode_ci NOT NULL,
  `date` datetime NOT NULL,
  `retweet` bigint(20) NOT NULL,
  `reply` bigint(20) NOT NULL,
  `quote` bigint(20) NOT NULL,
  `json` text COLLATE utf8mb4_unicode_ci NOT NULL,
  PRIMARY KEY (`id`),
  KEY `user_id` (`user_id`,`screen_name`),
  KEY `date` (`date`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
""")
db.commit()

cur.execute("""
CREATE TABLE IF NOT EXISTS `tweet_now` (
  `id` bigint(20) NOT NULL,
  `json` text COLLATE utf8mb4_unicode_ci NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

""")
db.commit()


cur.execute(" SELECT distinct(`id`) FROM `tweet` where retweet =0 and id not in (select id from  tweet_now) "
            " union"
            " SELECT distinct(`retweet`) FROM `tweet` where id not in (select id from  tweet_now) "
            " union "
            " SELECT distinct(`id`) FROM `tweet_reply` where id not in (select id from  tweet_now)")

tweets=cur.fetchall()
i=len(tweets)
print(len(tweets))

ids=[]
for tweet in tweets:
    ids.append(str(tweet[0]))

ids_to_retrieve=[]
while len(ids) > 0:
        print(len(ids))
        parameter = ','.join(ids[0:100]) #max 100 id per request
        try:
            place_endpoint ="https://api.twitter.com/1.1/statuses/lookup.json?id="+parameter
            response, data = client.request(place_endpoint)
            if response['status']=='200':
                if int(response['x-rate-limit-remaining'])<2:
                    print('id rescue: wait '+str( int(response['x-rate-limit-reset']) - int(time.time()) )+' seconds')
                    time.sleep(int(response['x-rate-limit-reset'])-int(time.time()))

            jsonTweets=json.loads(data.decode("utf-8"))

            for jsonTweet in jsonTweets:
                date=datetime.strptime(jsonTweet['created_at'],'%a %b %d %H:%M:%S +0000 %Y')\
                    .replace(tzinfo=tz.gettz('UTC'))\
                    .astimezone(tz.gettz('Europe/Rome'))


                cur.execute(" INSERT INTO `tweet_now` "
                        " (`id`, `json`) "
                        " VALUES "
                        "( %s, %s) on duplicate key update id=id",
                        (jsonTweet['id'], json.dumps(jsonTweet)))

                db.commit()

                if jsonTweet['retweet_count']==0:# or True:
                    if not jsonTweet.get('quoted_status'):
                        quote=0
                    else:
                        quote=jsonTweet['quoted_status']['id']

                    retweet=0

                    if jsonTweet['in_reply_to_status_id'] is None:
                        reply=0
                    else:
                        reply=jsonTweet['in_reply_to_status_id']

                    date=datetime.strptime(jsonTweet['created_at'],'%a %b %d %H:%M:%S +0000 %Y')\
                        .replace(tzinfo=tz.gettz('UTC'))\
                        .astimezone(tz.gettz('Europe/Rome'))

                    cur.execute(" INSERT INTO `tweet_retweet_original`(`id`, `user_id`,`screen_name`, `text`, `date`, `retweet`, `reply`,quote,json) "
                                " VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) on duplicate key  update id=id",
                    (jsonTweet['id'],
                     jsonTweet['user']['id'],
                     jsonTweet['user']['screen_name'],
                     jsonTweet['text'],
                     date.strftime("%Y-%m-%d %H:%M:%S"),
                     retweet,
                     reply,
                     quote,
                     json.dumps(jsonTweet)

                    ))

            print('id rescue: wait '+str((15*60)/int(response['x-rate-limit-limit']))+' seconds')
            time.sleep((15*60)/int(response['x-rate-limit-limit']))
            ids[0:100] =[]

        except:
            e = sys.exc_info()[0]
            print('exeption '+str(e))
            time.sleep(5)
            continue







