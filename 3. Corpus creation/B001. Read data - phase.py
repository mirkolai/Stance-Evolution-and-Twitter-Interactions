__author__ = 'mirko'
# -*- coding: utf-8 -*-
import os
import pymysql
import config as cfg

print(os.getcwd())

"""
This script aggregates all gathered data and it also slices tweets in four temporal phases.
The new table `tweet_phase` is introduced.
"""

db = pymysql.connect(host=cfg.mysql['host'], # your host, usually localhost
             user=cfg.mysql['user'], # your username
             passwd=cfg.mysql['passwd'], # your password
             db=cfg.mysql['db'],
             charset='utf8mb4',
             use_unicode=True) # name of the data base

db2 = pymysql.connect(host=cfg.mysql['host'], # your host, usually localhost
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


cur.execute("""
CREATE TABLE IF NOT EXISTS `tweet_phase` (
  `id` bigint(20) NOT NULL,
  `user_id` bigint(20) NOT NULL,
  `screen_name` varchar(250) COLLATE utf8mb4_unicode_ci NOT NULL,
  `text` varchar(2500) COLLATE utf8mb4_unicode_ci NOT NULL,
  `text_quote` varchar(2500) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `quote_id` bigint(20) DEFAULT NULL,
  `quote_user_id` bigint(20) DEFAULT NULL,
  `date` datetime NOT NULL,
  `phase` int(11) NOT NULL,
  `retweet` bigint(20) DEFAULT NULL,
  `retweet_user_id` bigint(20) DEFAULT NULL,
  `retweet_text` varchar(250) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `retweet_quote_text` varchar(250) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `retweet_date` datetime DEFAULT NULL,
  `retweet_phase` int(11) DEFAULT NULL,
  `reply` bigint(20) DEFAULT NULL,
  `reply_user_id` bigint(11) DEFAULT NULL,
  `reply_text` varchar(2500) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `reply_text_quote` varchar(2500) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `reply_date` datetime DEFAULT NULL,
  `reply_phase` bigint(11) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;""")
db.commit()

cur.execute("truncate table tweet_phase")
db.commit()

fastcur = db2.cursor(pymysql.cursors.SSCursor)

fastcur.execute(" SELECT `id`, `user_id`, `screen_name`, `text`, `date`, `retweet`, `reply`, quote FROM `tweet` "
            " union "
            " SELECT `id`, `user_id`, `screen_name`, `text`, `date`, `retweet`, `reply`, quote  FROM `tweet_retweet` "
            " union "
            " SELECT `id`, `user_id`, `screen_name`, `text`, `date`, `retweet`, `reply`, quote  FROM `tweet_retweet_original` "
            " union "
            " SELECT `id`, `user_id`, `screen_name`, `text`, `date`, `retweet`, `reply`, quote  FROM `tweet_reply` ")

for result in fastcur:


        id=result[0]
        user_id=result[1]
        screen_name=result[2]
        text=result[3]
        date=result[4]
        phase=cfg.get_phase(date)
        print(date,phase)

        phasezero=0


        if phase==0:
            phasezero+=1

        if phase!=0:

            is_retweet=result[5]
            retweet=None
            retweet_user_id=None
            retweet_text=None
            retweet_date=None
            retweet_phase=None
            retweet_quote_text=None
            if is_retweet != 0:

                cur.execute(" SELECT `id`, `user_id`, `text`, `date`,quote "
                            " FROM `tweet_retweet_original` WHERE `id`=%s",(is_retweet))
                retweet_result=cur.fetchone()

                if retweet_result is not None:
                    retweet=retweet_result[0]
                    retweet_user_id=retweet_result[1]
                    retweet_text=retweet_result[2]
                    retweet_date=retweet_result[3]
                    retweet_phase=cfg.get_phase(retweet_date)

                    retweet_is_quote=retweet_result[4]
                    retweet_quote=None
                    retweet_quote_text=None
                    if retweet_is_quote != 0:

                        cur.execute(" SELECT `id`, `user_id`, `text`, `date` "
                                    " FROM `tweet_quote` WHERE `id`=%s",(retweet_is_quote))
                        retweet_quote_result=cur.fetchone()

                        if retweet_quote_result is not None:
                            retweet_quote_text=retweet_quote_result[2]



                else:
                    retweet=-1

            ######################################################################à
            is_reply=result[6]
            reply=None
            reply_user_id=None
            reply_text=None
            reply_date=None
            reply_phase=None
            reply_quote_text=None
            if is_reply != 0:

                cur.execute(" SELECT `id`, `user_id`, `text`, `date`,quote "
                            " FROM `tweet_reply` WHERE `id`=%s",(is_reply))
                reply_result=cur.fetchone()

                if reply_result is not None:
                    reply=reply_result[0]
                    reply_user_id=reply_result[1]
                    reply_text=reply_result[2]
                    reply_date=reply_result[3]
                    reply_phase=cfg.get_phase(reply_date)

                    reply_is_quote=reply_result[4]
                    reply_quote=None
                    reply_quote_text=None
                    if reply_is_quote != 0:

                        cur.execute(" SELECT `id`, `user_id`, `text`, `date` "
                                    " FROM `tweet_quote` WHERE `id`=%s",(reply_is_quote))
                        reply_quote_result=cur.fetchone()

                        if reply_quote_result is not None:
                            reply_quote_text=reply_quote_result[2]



                else:
                    reply=-1


            ##########################################
            is_quote=result[7]
            quote=None
            quote_text=None
            quote_id=None
            quote_user_id=None
            if is_quote != 0:

                cur.execute(" SELECT `id`, `user_id`, `text`, `date` "
                            " FROM `tweet_quote` WHERE `id`=%s",(is_quote))
                quote_result=cur.fetchone()

                if quote_result is not None:
                    quote_text=quote_result[2]
                    quote_id=quote_result[0]
                    quote_user_id=quote_result[1]

            cur.execute(" INSERT INTO `tweet_phase` "
                        " (`id`, `user_id`, `screen_name`, `text`, `text_quote`, quote_id, quote_user_id, `date`, `phase`, "
                        " `retweet`, retweet_user_id, retweet_text, retweet_quote_text, retweet_date, retweet_phase,"
                        " `reply`, `reply_user_id`, `reply_text`, `reply_text_quote`, `reply_date`, `reply_phase`)"
                        " values "
                        " (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
                        " on duplicate key  update id=id"
                        "",
            (id, user_id, screen_name, text, quote_text, quote_id, quote_user_id, date, phase,
             retweet, retweet_user_id, retweet_text, retweet_quote_text, retweet_date, retweet_phase,
             reply, reply_user_id, reply_text, reply_quote_text, reply_date, reply_phase
            ))
            db.commit()


