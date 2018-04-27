import numpy
from sklearn.ensemble.forest import RandomForestClassifier
from sklearn.svm.classes import SVC

__author__ = 'mirko'
import Features_manager
import Database_manager
import config as cfg
import pymysql
from collections import Counter


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


print("Task A - SVM")


database_manager=Database_manager.make_database_manager()
feature_manager=Features_manager.make_feature_manager()

tweets_training=numpy.array(database_manager.return_tweets())
tweets_test=numpy.array(database_manager.return_tweets_test())

stance=numpy.array(feature_manager.get_stance(tweets_training))


count = Counter(stance)
print("Count most common",count.most_common())


features_set=numpy.array(
["hashtagplus","hashtagplusreply", "mentionplus"])


stuff = range(0, len(features_set) )
X_train,X_test, feature_name_global,feature_index_global=feature_manager.create_feature_space(tweets_training,features_set,tweet_test=tweets_test)

clf = SVC(kernel='linear')

clf.fit(X_train,stance)
test_predict = clf.predict(X_test)

for i in range(0,len(tweets_test)):

    cur.execute("""UPDATE `corpus_automatic_stance` SET
    stance=%s
    where id_tweet=%s""",
    (test_predict[i],tweets_test[i].id ))
    db.commit()


count = Counter(test_predict)
print("Count most common",count.most_common())
