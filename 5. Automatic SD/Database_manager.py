__author__ = 'mirko'

from Tweet import make_tweet
import pymysql
import config as cfg

class Database_manager(object):

    db=None
    cur=None
    def __init__(self):

        self.db = pymysql.connect(host=cfg.mysql['host'],
                 user=cfg.mysql['user'],
                 passwd=cfg.mysql['passwd'],
                 db=cfg.mysql['db'],
                 charset='utf8')
        self.cur = self.db.cursor()
        self.cur.execute('SET NAMES utf8mb4')
        self.cur.execute("SET CHARACTER SET utf8mb4")
        self.cur.execute("SET character_set_connection=utf8mb4")
        self.db.commit()

    def return_tweets(self,phase=None):


        #if os.path.isfile('tweets.pkl') :
        #    tweets= joblib.load('tweets.pkl')
        #    return tweets


        tweets=[]
        if phase is None:
            self.cur.execute(" SELECT  `id_tweet`,  `text_tweet`, `text_retweet`, `text_reply`, `text_reply_to`, `stance`, `phase`, `user_id` FROM `corpus_stance` where stance is None")
        else:
            self.cur.execute(" SELECT  `id_tweet`,  `text_tweet`, `text_retweet`, `text_reply`, `text_reply_to`, `stance`, `phase`, `user_id` FROM `corpus_stance` where stance is None and phase=%s",phase)
        i=0
        for row in self.cur.fetchall():
                i+=1
                id=row[0]
                tweet=row[1]
                retweet=row[2]
                reply=row[3]
                reply_to=row[4]
                stance=row[5]
                phase=row[6]
                user_id=row[7]

                this_tweet=make_tweet(self,id, user_id, tweet, retweet, reply, reply_to,  stance, phase )

                tweets.append(this_tweet)

        #joblib.dump(tweets, 'tweets.pkl')

        return tweets




    def return_tweets_test(self,phase=None):


        #if os.path.isfile('tweets.pkl') :
        #    tweets= joblib.load('tweets.pkl')
        #    return tweets


        tweets=[]
        if phase is None:
            self.cur.execute(" SELECT  `id_tweet`, `text_tweet`, `text_retweet`, `text_reply`, `text_reply_to`, `stance`, `phase`, `user_id` FROM `corpus_automatic_stance` where stance !='disagreement'")
        else:
            self.cur.execute(" SELECT  `id_tweet`,  `text_tweet`, `text_retweet`, `text_reply`, `text_reply_to`, `stance`, `phase`, `user_id` FROM `corpus_automatic_stance` where stance !='disagreement' and phase=%s",phase)
        i=0
        for row in self.cur.fetchall():
                i+=1
                id=row[0]
                tweet=row[1]
                retweet=row[2]
                reply=row[3]
                reply_to=row[4]
                stance=row[5]
                phase=row[6]
                user_id=row[7]

                this_tweet=make_tweet(object,id, user_id, tweet, retweet, reply, reply_to,  stance, phase )

                tweets.append(this_tweet)

        #joblib.dump(tweets, 'tweets.pkl')

        return tweets


    def return_community(self,network,user_id,phase=None):
            where="WHERE id="+str(user_id)
            if phase  is not None:
                where+=" AND phase ="+str(phase)
            self.cur.execute(" SELECT community "
                             " FROM `user_"+network+"_relation_communities` "
                             + where)

            result=self.cur.fetchone()
            if result is  not None:
                return result[0]
            else:
                return  None
def make_database_manager():
    database_manager = Database_manager()

    return database_manager


if __name__ == '__main__':
    database_manager = Database_manager()
    tweets=database_manager.return_tweets()

