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
All retweets for each retweeted tweet are recovered.
The table tweet_retweet is introduced

"""

cur.execute("""
CREATE TABLE IF NOT EXISTS `tweet_retweet` (
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
  KEY `user_id` (`user_id`,`screen_name`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
""")
db.commit()

fastcur = db.cursor(pymysql.cursors.SSCursor)
fastcur.execute("""
select * from tweet_now where id not  in  (select retweet from tweet_retweet)
""")

ids_str=[]
for tweet in fastcur:
    if json.loads(tweet[1])['retweet_count']>0:
        ids_str.append(str(tweet[0]))


i=len(ids_str)
for id_str in ids_str:
    print(i)
    i=i-1
    max_id=-1
    while max_id!=0:

        if max_id<0:

            timeline_endpoint = "https://api.twitter.com/1.1/statuses/retweets/"+id_str+".json"
        else:
            timeline_endpoint = "https://api.twitter.com/1.1/statuses/retweets/"+id_str+".json?max_id="+str(max_id)

        #print(timeline_endpoint)
        try:
            response, data = client.request(timeline_endpoint)
            if response['status']=='200':
                if int(response['x-rate-limit-remaining'])<2:
                    print('id rescue: wait '+str(int(response['x-rate-limit-reset'])-int(time.time()))+' seconds')
                    time.sleep(int(response['x-rate-limit-reset'])-int(time.time()))

                max_id = 0
                tweets = json.loads(data.decode("utf-8"))
                print(len(tweets))
                for jsonTweet in tweets:

                    if jsonTweet.get('quoted_status'):
                        quote=jsonTweet['quoted_status']['id']
                    else:
                        quote=0

                    if not jsonTweet.get('retweeted_status'):
                        retweet=0
                    else:
                        retweet=jsonTweet['retweeted_status']['id']

                    if jsonTweet['in_reply_to_status_id'] is None:
                        reply=0
                    else:
                        reply=jsonTweet['in_reply_to_status_id']


                    date=datetime.strptime(jsonTweet['created_at'],'%a %b %d %H:%M:%S +0000 %Y')\
                        .replace(tzinfo=tz.gettz('UTC'))\
                        .astimezone(tz.gettz('Europe/Rome'))

                    cur.execute("INSERT INTO `tweet_retweet`"
                                " (`id`, `user_id`, `screen_name`, `text`, `date`, `retweet`, `reply`, `quote`, `json`) "
                                " VALUES "
                                " (%s,%s,%s,%s,%s,%s,%s,%s,%s) "
                                " on duplicate key update id=id",
                                (jsonTweet['id'],jsonTweet['user']['id'],jsonTweet['user']['screen_name'],jsonTweet['text'],date, retweet,reply, quote, json.dumps(jsonTweet)))
                    db.commit()

                    if max_id==0:
                        max_id=int(jsonTweet['id'])-1
                    elif max_id>int(jsonTweet['id']):
                        max_id=int(jsonTweet['id'])-1

                    if not jsonTweet['retweeted_status'].get('quoted_status'):
                        quote=0
                    else:
                        quote=jsonTweet['retweeted_status']['quoted_status']['id']

                    retweet=0

                    if jsonTweet['retweeted_status']['in_reply_to_status_id'] is None:
                        reply=0
                    else:
                        reply=jsonTweet['retweeted_status']['in_reply_to_status_id']

                    date=datetime.strptime(jsonTweet['retweeted_status']['created_at'],'%a %b %d %H:%M:%S +0000 %Y')\
                        .replace(tzinfo=tz.gettz('UTC'))\
                        .astimezone(tz.gettz('Europe/Rome'))

                    cur.execute(" INSERT INTO `tweet_retweet_original`(`id`, `user_id`,`screen_name`, `text`, `date`, `retweet`, `reply`,quote,json) "
                                " VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) on duplicate key  update id=id",
                    (jsonTweet['retweeted_status']['id'],
                     jsonTweet['retweeted_status']['user']['id'],
                     jsonTweet['retweeted_status']['user']['screen_name'],
                     jsonTweet['retweeted_status']['text'],
                     date.strftime("%Y-%m-%d %H:%M:%S"),
                     retweet,
                     reply,
                     quote,
                     json.dumps(jsonTweet['retweeted_status'])

                    ))
                    db.commit()
                print(len(tweets))
                if len(tweets)<80:
                    max_id=0

                print('id rescue: wait '+str((15*60)/int(response['x-rate-limit-limit']))+' seconds')
                print('x-rate-limit-remaining  : '+str(response['x-rate-limit-remaining']))
                time.sleep((15*60)/int(response['x-rate-limit-limit']))

            elif response['status']==400 or response['status']==403 or response['status']==404 or response['status']==401:
                print(response['status'])
                max_id=0
            else:
                print(response['status'])
                max_id=0

        except:
            e = sys.exc_info()[0]
            print('exeption '+str(e))
            time.sleep(60)
            continue

