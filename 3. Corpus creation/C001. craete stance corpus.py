__author__ = 'mirko'
import config as cfg
import pymysql

"""
It retrieves a triplet for each user for each temporal phase and it save triplets in the table  `corpus_stance`

"""

db = pymysql.connect(host=cfg.mysql['host'], # your host, usually localhost
             user=cfg.mysql['user'], # your username
             passwd=cfg.mysql['passwd'], # your password
             db=cfg.mysql['db'],
             charset='utf8') # name of the data base

cur = db.cursor()
cur.execute('SET NAMES utf8mb4')
cur.execute("SET CHARACTER SET utf8mb4")
cur.execute("SET character_set_connection=utf8mb4")
db.commit()

cur.execute("""CREATE TABLE IF NOT EXISTS `corpus_stance` (
  `user_id` bigint(20) NOT NULL,
  `screen_name` text COLLATE utf8mb4_unicode_ci NOT NULL,
  `phase` int(11) NOT NULL,
  `id_tweet` bigint(20) NOT NULL,
  `text_tweet` varchar(200) COLLATE utf8mb4_unicode_ci NOT NULL,
  `text_tweet_quote` varchar(250) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `id_retweet` bigint(20) NOT NULL,
  `text_retweet` varchar(200) COLLATE utf8mb4_unicode_ci NOT NULL,
  `text_retweet_quote` varchar(250) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `id_reply` bigint(20) NOT NULL,
  `text_reply` varchar(200) COLLATE utf8mb4_unicode_ci NOT NULL,
  `text_reply_to` text COLLATE utf8mb4_unicode_ci NOT NULL,
  `text_reply_to_quote` varchar(250) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `stance` varchar(250) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT 'ND',
  `annotators` int(11) NOT NULL DEFAULT '0',
  `support` int(11) NOT NULL DEFAULT '0',
  PRIMARY KEY (`user_id`,`phase`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;""")
db.commit()

cur.execute("truncate table  `corpus_stance`")
db.commit()


cur.execute("""
SELECT id
FROM  `user`
WHERE
    `phase_1_tweet` >0
AND `phase_1_retweet` >0
AND  `phase_1_reply` >0
AND  `phase_2_tweet` >0
AND  `phase_2_retweet` >0
AND  `phase_2_reply` >0
AND  `phase_3_tweet` >0
AND  `phase_3_retweet` >0
AND  `phase_3_reply` >0
AND  `phase_4_tweet` >0
AND  `phase_4_retweet` >0
AND  `phase_4_reply` >0""")
users=cur.fetchall()

ids=[]
for user in users:
    ids.append(user[0])

i=len(ids)
for id in ids:
    i-=1
    print("remain  "+str(i))
    for phase in [1,2,3,4]:
        #tweet
        cur.execute("""
        SELECT `id`, `user_id`, `screen_name`, `text`, `phase`, `retweet`, `reply`,
        `text_quote`
         FROM `tweet_phase`  WHERE
         user_id=%s and phase =%s and `retweet` is NULL and `reply` is NULL
         order by rand()
         """,(id,phase))
        tweet=cur.fetchone()

        #retweet
        cur.execute("""
        SELECT `id`, `user_id`, `screen_name`, `text`, `phase`, `retweet`, `reply`,
        `retweet_quote_text`
         FROM `tweet_phase`  WHERE
         user_id=%s and phase =%s and `retweet` is not NULL and `reply` is NULL
         order by rand()
         """,(id,phase))
        retweet=cur.fetchone()

        #reply
        cur.execute("""
        SELECT `id`, `user_id`, `screen_name`, `text`, `phase`, `retweet`, `reply`,`reply_text`,
        `reply_text_quote`
         FROM `tweet_phase`  WHERE
         user_id=%s and phase =%s and `retweet` is NULL and `reply` is not NULL and reply!=-1
         order by rand()
         """,(id,phase))
        reply=cur.fetchone()


        cur.execute("""INSERT INTO `corpus_stance`
        (`user_id`, `screen_name`, `phase`, `id_tweet`, `text_tweet`, `text_tweet_quote`,
        `id_retweet`, `text_retweet`, `text_retweet_quote`,
        `id_reply`, `text_reply`,`text_reply_to`, `text_reply_to_quote`) VALUES
         (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
        (tweet[1],tweet[2],tweet[4],tweet[0],tweet[3],tweet[7],
         retweet[0],retweet[3],retweet[7],
         reply[0],reply[3],reply[7],reply[8]))
        db.commit()

