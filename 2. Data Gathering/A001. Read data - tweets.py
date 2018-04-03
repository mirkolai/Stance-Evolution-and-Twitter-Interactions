__author__ = 'mirko'
# -*- coding: utf-8 -*-
import  sys
import pymysql
import json
import config as cfg
import glob
from datetime import datetime
from dateutil import tz

db = pymysql.connect(host=cfg.mysql['host'],
                     user=cfg.mysql['user'],
                     passwd=cfg.mysql['passwd'],
                     db=cfg.mysql['db'],
                     charset='utf8mb4',
                     use_unicode=True)
cur = db.cursor()
cur.execute('SET NAMES utf8mb4')
cur.execute("SET CHARACTER SET utf8mb4")
cur.execute("SET character_set_connection=utf8mb4")
db.commit()

"""
Recovering json from the directory data, I store tweets in a more convenient way.
I usually use mysql.

In this script you need a new table. The table `tweet`.
"""

cur.execute("""CREATE TABLE IF NOT EXISTS `tweet` (
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
) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;""")
db.commit()

cur.execute("truncate table `tweet`")
db.commit()

filelist = sorted(glob.glob("../data/*json"))

for file in filelist:
    print(file)
    infile = open(file, 'r')

    for row in infile:
        ror=row.encode("utf-8")

        try:
            data = json.loads(row)
        except:
            e = sys.exc_info()[0]
            print('exeption '+str(e))
            pass

        if data.get('lang'):
            if data['lang']=='it':

                if not data.get('quoted_status'):
                    quote=0
                else:
                    quote=data['quoted_status']['id']

                if not data.get('retweeted_status'):
                    retweet=0
                else:
                    retweet=data['retweeted_status']['id']


                if data['in_reply_to_status_id'] is None:
                    reply=0
                else:
                    reply=data['in_reply_to_status_id']

                date=datetime.strptime(data['created_at'],'%a %b %d %H:%M:%S +0000 %Y')\
                    .replace(tzinfo=tz.gettz('UTC'))\
                    .astimezone(tz.gettz('Europe/Rome'))

                cur.execute(" INSERT INTO `tweet`(`id`, `user_id`,`screen_name`, `text`, `date`, `retweet`, `reply`,quote,json) "
                            " VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) on duplicate key  update id=id",
                (data['id'],
                 data['user']['id'],
                 data['user']['screen_name'],
                 data['text'],
                 date.strftime("%Y-%m-%d %H:%M:%S"),
                 retweet,
                 reply,
                 quote,
                 json.dumps(data)

                ))
                db.commit()



