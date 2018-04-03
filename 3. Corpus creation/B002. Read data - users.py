__author__ = 'mirko'
# -*- coding: utf-8 -*-
import pymysql
import config as cfg

"""
This script counts the number of tweets, retweets, and replies wrote by each user for each phase.
"""

db = pymysql.connect(host=cfg.mysql['host'],user=cfg.mysql['user'],passwd=cfg.mysql['passwd'],db=cfg.mysql['db'],charset='utf8')
cur = db.cursor()
cur.execute('SET NAMES utf8mb4')
cur.execute("SET CHARACTER SET utf8mb4")
cur.execute("SET character_set_connection=utf8mb4")
db.commit()




cur.execute("""CREATE TABLE IF NOT EXISTS `user` (
  `id` bigint(20) NOT NULL,
  `screen_name` varchar(25) COLLATE utf8mb4_unicode_ci NOT NULL,
  `phase_1_tweet` int(11) NOT NULL DEFAULT '0',
  `phase_1_retweet` int(11) NOT NULL DEFAULT '0',
  `phase_1_reply` int(11) NOT NULL DEFAULT '0',
  `phase_1_count` int(11) NOT NULL DEFAULT '0',
  `phase_2_tweet` int(11) NOT NULL DEFAULT '0',
  `phase_2_retweet` int(11) NOT NULL DEFAULT '0',
  `phase_2_reply` int(11) NOT NULL DEFAULT '0',
  `phase_2_count` int(11) NOT NULL DEFAULT '0',
  `phase_3_tweet` int(11) NOT NULL DEFAULT '0',
  `phase_3_retweet` int(11) NOT NULL DEFAULT '0',
  `phase_3_reply` int(11) NOT NULL DEFAULT '0',
  `phase_3_count` int(11) NOT NULL DEFAULT '0',
  `phase_4_tweet` int(11) NOT NULL DEFAULT '0',
  `phase_4_retweet` int(11) NOT NULL DEFAULT '0',
  `phase_4_reply` int(11) NOT NULL DEFAULT '0',
  `phase_4_count` int(11) DEFAULT '0',
  PRIMARY KEY (`id`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
""")
db.commit()

cur.execute(" truncate table `user` ")
db.commit()

cur.execute("SELECT distinct `user_id`, `screen_name`, `phase`, `retweet`, `reply` FROM `tweet_phase`")
users=cur.fetchall()

for user in users:

    if user[3]!=-1 and user[4]!=-1:
        user_id=user[0]
        screen_name=user[1]
        phase= user[2]
        retweet = 1 if user[3] is not None else  0
        reply   = 1 if user[4] is not None else  0

        tweet=0

        if reply == 0 and retweet == 0: #reply 0 not reply -1 reply not found
            tweet = 1
        count=1

        cur.execute(" INSERT INTO `user`"
                    " (`id`, `screen_name`, "
                    " `phase_"+str(phase)+"_tweet`, `phase_"+str(phase)+"_retweet`, `phase_"+str(phase)+"_reply`, `phase_"+str(phase)+"_count`) "
                    " VALUES "
                    " (%s,%s,%s,%s,%s,%s)"
                    " on duplicate key update"
                    " `phase_"+str(phase)+"_tweet`=`phase_"+str(phase)+"_tweet`+%s, "
                    " `phase_"+str(phase)+"_retweet`=`phase_"+str(phase)+"_retweet`+%s, "
                    " `phase_"+str(phase)+"_reply`=`phase_"+str(phase)+"_reply`+%s, "
                    " `phase_"+str(phase)+"_count`=`phase_"+str(phase)+"_count`+%s ",
        (
         user_id,
         screen_name,
         tweet,
         retweet,
         reply,
         count,
         tweet,
         retweet,
         reply,
         count,

        ))
        db.commit()
