__author__ = 'mirko'
# -*- coding: utf-8 -*-
import pymysql
import config as cfg


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
Yuo need to import crowdflower_output.txt (the result of the annotation performed on crowdflower) in the table crowdflower.
The script return the gold standard.

"""

cur.execute("""SELECT distinct id_tweet from corpus_stance """)
tweets=cur.fetchall()
agreement={}
for tweet in tweets:

    cur.execute("""SELECT  `stance` ,  `id_tweet` ,  `_golden` , _worker_id
    FROM  `crowdflower`
    where id_tweet = %s """,(tweet[0]))
    annotations=cur.fetchall()

    for annotation in annotations:

        if "false" in annotation[2]:
            if annotation[0] in agreement:
                agreement[annotation[0]]+=1
            else:
                agreement[annotation[0]]=1

    support=0
    stance=""
    annotators=0
    for k, value in agreement.items():
        annotators+=value
        if value>support:
            support=value
            stance=k

    if support/annotators<=0.5:
        stance="disagreement"
        print(annotation[1],agreement)

    cur.execute("""UPDATE `corpus_stance` SET
  `stance`=%s,`annotators`= %s, support=%s
  where id_tweet =%s

  """,(stance,annotators,support,annotation[1]))
    db.commit()

    agreement={}


