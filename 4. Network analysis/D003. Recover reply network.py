__author__ = 'mirko'
import json
import config as cfg
import pymysql
from datetime import datetime
from dateutil import tz


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


cur.execute("""CREATE TABLE IF NOT EXISTS `user_reply_relation` (
  `source` bigint(20) NOT NULL,
  `target` bigint(20) NOT NULL,
  `phase` int(11) NOT NULL,
  `count` int(11) NOT NULL DEFAULT '1',
  PRIMARY KEY (`source`,`target`,`phase`),
  KEY `target` (`target`),
  KEY `source` (`source`),
  KEY `target_2` (`target`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
""")
db.commit()
cur.execute("truncate table `user_reply_relation`")
db.commit()


cur.execute("""
SELECT  `user_id`, `date`, `reply_user_id` FROM  `tweet_phase` WHERE `reply_user_id` is not NULL
""")
tweets=cur.fetchall()


for tweet in tweets:

    phase=cfg.get_phase(tweet[1])
    source=tweet[0]
    target=tweet[2]

    cur.execute(" INSERT INTO `user_reply_relation`(`source`, `target`, `phase`, `count`) VALUES (%s,%s,%s,1)  on duplicate key update count=count+1",
    (source,target,phase))
    db.commit()




