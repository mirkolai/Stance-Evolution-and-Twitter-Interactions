__author__ = 'mirko'
__author__ = 'mirko'
import pymysql
import config as cfg
import oauth2 as oauth
import re
import time
import json


db = pymysql.connect(host=cfg.mysql['host'], # your host, usually localhost
             user=cfg.mysql['user'], # your username
             passwd=cfg.mysql['passwd'], # your password
             db=cfg.mysql['db'],
             charset='utf8') # name of the data base

consumer = oauth.Consumer(key=cfg.twitter['CONSUMER_KEY'], secret=cfg.twitter['CONSUMER_SECRET'])
access_token = oauth.Token(key=cfg.twitter['ACCESS_KEY'], secret=cfg.twitter['ACCESS_SECRET'])
client = oauth.Client(consumer, access_token)


cur = db.cursor()
cur.execute('SET NAMES utf8mb4')
cur.execute("SET CHARACTER SET utf8mb4")
cur.execute("SET character_set_connection=utf8mb4")
db.commit()




cur.execute("select text from tweet")
tweets=cur.fetchall()

for tweet in tweets:
    for user in re.findall(r"@(\w+)", tweet[0]):
        #print(user)

        cur.execute(" select * from `dictionary_mentions` where screen_name=%s",(user))
        if cur.fetchone()== None:

            place_endpoint = "https://api.twitter.com/1.1/users/show.json?screen_name="+user

            response, data = client.request(place_endpoint)
            #print(response)
            #print(response['status'])
            #print(response['x-rate-limit-limit'])
            print(data)
            if response['status']=='200':
                if int(response['x-rate-limit-remaining'])<1:
                    print('id rescue: wait '+str( int(response['x-rate-limit-reset']) - int(time.time()) )+' seconds')
                    time.sleep(int(response['x-rate-limit-reset'])-int(time.time()))

            jsonuser=json.loads(data.decode("utf-8"))
            if "id" in jsonuser:

                cur.execute("INSERT INTO `dictionary_mentions`(`id`, `screen_name`, `name`, `description`, `place`, `json`,language)"
                            " VALUES"
                            " (%s,%s,%s,%s,%s,%s,%s)"
                            " on duplicate key update id=id",(jsonuser['id'],jsonuser['screen_name'],jsonuser['name'],jsonuser['description'],jsonuser['location'],json.dumps(jsonuser),"it"))
                db.commit()
            print(json.dumps(jsonuser,2))
            print('id rescue: wait '+str((15*60)/int(response['x-rate-limit-limit']))+' seconds')
            time.sleep((15*60)/int(response['x-rate-limit-limit']))


