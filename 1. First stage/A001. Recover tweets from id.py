__author__ = 'mirko'
"""
I can't share tweets so I shared the ids list of all tweets gathered with Stream API.
In order to replicate the experiment, you need to recover the JSON for each tweet.
You could use the following script.
Data are stored in ../data directory.
The file tweets.json will consist in a json tweet for each row.
"""
import sys
import oauth2 as oauth
import time
import json
import config as cfg

CONSUMER_KEY    = cfg.twitter["CONSUMER_KEY"]
CONSUMER_SECRET = cfg.twitter["CONSUMER_SECRET"]
ACCESS_KEY      = cfg.twitter["ACCESS_KEY"]
ACCESS_SECRET   = cfg.twitter["ACCESS_SECRET"]

consumer = oauth.Consumer(key=CONSUMER_KEY, secret=CONSUMER_SECRET)
access_token = oauth.Token(key=ACCESS_KEY, secret=ACCESS_SECRET)
client = oauth.Client(consumer, access_token)

ids=[]
file = open("ids.txt", 'r')
for row in file:
    ids.append(row.replace("\"","").replace("\n",""))
file.close()

file = open("../data/tweets.json", 'w')

while len(ids) > 0:
        parameter = ','.join(ids[0:100]) #max 100 id per request
        ids[0:100] =[]
        try:
            place_endpoint ="https://api.twitter.com/1.1/statuses/lookup.json?id="+parameter
            response, data = client.request(place_endpoint)
            if response['status']=='200':
                if int(response['x-rate-limit-remaining'])<2:
                    print('id rescue: wait '+str( int(response['x-rate-limit-reset']) - int(time.time()) )+' seconds')
                    time.sleep(int(response['x-rate-limit-reset'])-int(time.time()))

            jsonTweet=json.loads(data.decode("utf-8"))
            for tweet in jsonTweet:
                #print(json.dumps(tweet))
                file.write(json.dumps(tweet)+"\n")

            print('id rescue: wait '+str((15*60)/int(response['x-rate-limit-limit']))+' seconds')
            time.sleep((15*60)/int(response['x-rate-limit-limit']))
        except:
            e = sys.exc_info()[0]
            print('exeption '+str(e))
            time.sleep(60)
file.close()