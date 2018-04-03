__author__ = 'mirko'
# -*- coding: utf-8 -*-
import pymysql
import config as cfg

"""
This  script calculate the IAA

"""

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

cur.execute("""SELECT   `id_tweet` , `stance` ,  phase
    FROM  `crowdflower`
    where _golden="false" """)
rows=cur.fetchall()
tweets={}
for row in rows:
    if row[0] not in tweets:
        tweets[row[0]]=[]
    tweets[row[0]].append(row[1])


agreement=0
notagreement=0
for tweet, value in  tweets.items():

    for i in range(0,len(value)):
        for j in range(i,len(value)):
            if i!=j:
                if value[i]!=value[j]:
                    notagreement+=1
                else:
                    agreement+=1



print(agreement,notagreement)

print(agreement/(notagreement+agreement))


for p in range(1,5):

    tweets={}
    for row in rows:
        if row[0] not in tweets:
            tweets[row[0]]=[]

        if row[2]==p:
            tweets[row[0]].append(row[1])


    agreement=0
    notagreement=0
    for tweet, value in  tweets.items():

        for i in range(0,len(value)):
            for j in range(i,len(value)):
                if i!=j:
                    if value[i]!=value[j]:
                        notagreement+=1
                    else:
                        agreement+=1


    print("phase",p)
    print(agreement,notagreement)

    print(agreement/(notagreement+agreement))

