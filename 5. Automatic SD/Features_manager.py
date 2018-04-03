__author__ = 'mirko'

from sklearn.feature_extraction.text import  CountVectorizer
import numpy as np
import re
from scipy.sparse import csr_matrix, hstack
from Linguistic_resources import WORDLIST


class Features_manager(object):

    def __init__(self):


        return

    def get_stance(self,tweets):

        stance  = []

        for tweet in tweets:
            stance.append(tweet.stance)


        return stance


    #features extractor
    def create_feature_space(self,tweets,featureset,tweet_test=None):


        global_featureset={
            "BoW"  : self.get_BoW_features(tweets,tweet_test),
            "hashtag"  : self.get_hashtag_features(tweets,tweet_test),
            "hashtagplus"  : self.get_hashtagplus_features(tweets,tweet_test),
            "hashtagplusreply"  : self.get_hashtagplusreply_features(tweets,tweet_test),
            "mention"  : self.get_mention_features(tweets,tweet_test),
            "mentionplus"  : self.get_mentionplus_features(tweets,tweet_test),
            "mentionplusreply"  : self.get_mentionplusreply_features(tweets,tweet_test),

            "community_friend"  : self.get_communityfriend_features(tweets,tweet_test),
            "community_quote"  : self.get_communityquote_features(tweets,tweet_test),
            "community_reply"  : self.get_communityreply_features(tweets,tweet_test),
            "community_retweet"  : self.get_communityretweet_features(tweets,tweet_test),


        }

        if tweet_test is None:
            all_feature_names=[]
            all_feature_index=[]
            all_X=[]
            index=0
            for key in featureset:
                X,feature_names=global_featureset[key]

                current_feature_index=[]
                for i in range(0,len(feature_names)):
                    current_feature_index.append(index)
                    index+=1
                all_feature_index.append(current_feature_index)

                all_feature_names=np.concatenate((all_feature_names,feature_names))
                if all_X!=[]:
                    all_X=csr_matrix(hstack((all_X,X)))
                else:
                    all_X=X

            return all_X, all_feature_names, np.array(all_feature_index)
        else:
            all_feature_names=[]
            all_feature_index=[]
            all_X=[]
            all_X_test=[]
            index=0
            for key in featureset:

                X,X_test,feature_names=global_featureset[key]

                current_feature_index=[]
                for i in range(0,len(feature_names)):
                    current_feature_index.append(index)
                    index+=1
                all_feature_index.append(current_feature_index)

                all_feature_names=np.concatenate((all_feature_names,feature_names))
                if all_X!=[]:
                    all_X=csr_matrix(hstack((all_X,X)))
                    all_X_test=csr_matrix(hstack((all_X_test,X_test)))
                else:
                    all_X=X
                    all_X_test=X_test

            return all_X, all_X_test, all_feature_names, np.array(all_feature_index)

    def get_BoW_features(self, tweets,tweet_test=None):

        tfidfVectorizer = CountVectorizer(ngram_range=(1,3),
                                          lowercase=True,
                                          binary=True,
                                          max_features=500000)


        if tweet_test is None:
            feature  = []
            for tweet in tweets:

                feature.append(re.sub(r"#(\w+)"," ", tweet.tweet))

            tfidfVectorizer = tfidfVectorizer.fit(feature)

            X = tfidfVectorizer.transform(feature)

            feature_names=tfidfVectorizer.get_feature_names()

            return X, feature_names
        else:
            feature  = []
            feature_test  = []
            for tweet in tweets:
                feature.append(re.sub(r"#(\w+)"," ", tweet.tweet))

            for tweet in tweet_test:
                feature_test.append(re.sub(r"#(\w+)"," ", tweet.tweet))


            tfidfVectorizer = tfidfVectorizer.fit(feature)

            X_train = tfidfVectorizer.transform(feature)
            X_test = tfidfVectorizer.transform(feature_test)

            feature_names=tfidfVectorizer.get_feature_names()

            return X_train, X_test, feature_names


    def get_hashtag_features(self, tweets,tweet_test=None):

        tfidfVectorizer = CountVectorizer(ngram_range=(1,1),
                                          lowercase=True,
                                          binary=False,
                                          max_features=500000)


        if tweet_test is None:
            feature  = []
            for tweet in tweets:

                feature.append((' '.join(re.findall(r"#(\w+)", tweet.tweet))))

            tfidfVectorizer = tfidfVectorizer.fit(feature)

            X = tfidfVectorizer.transform(feature)

            feature_names=tfidfVectorizer.get_feature_names()

            return X, feature_names
        else:
            feature  = []
            feature_test  = []
            for tweet in tweets:

                feature.append((' '.join(re.findall(r"#(\w+)", tweet.tweet))))

            for tweet in tweet_test:
                feature_test.append((' '.join(re.findall(r"#(\w+)", tweet.tweet))))


            tfidfVectorizer = tfidfVectorizer.fit(feature)

            X_train = tfidfVectorizer.transform(feature)
            X_test = tfidfVectorizer.transform(feature_test)

            feature_names=tfidfVectorizer.get_feature_names()

            return X_train, X_test, feature_names



    def get_hashtagplus_features(self, tweets,tweet_test=None):


        tfidfVectorizer = CountVectorizer(ngram_range=(2,2),
                                          lowercase=True,
                                          binary=True,
                                          max_features=500000)

        if tweet_test is None:
            feature  = []
            for tweet in tweets:

                feature.append(tweet.hashtagplus)


            tfidfVectorizer = tfidfVectorizer.fit(feature)

            X = tfidfVectorizer.transform(feature)

            feature_names=tfidfVectorizer.get_feature_names()

            return X, feature_names
        else:
            feature  = []
            feature_test  = []
            for tweet in tweets:
                feature.append(tweet.hashtagplus)

            for tweet in tweet_test:
                feature_test.append(tweet.hashtagplus)


            tfidfVectorizer = tfidfVectorizer.fit(feature)

            X_train = tfidfVectorizer.transform(feature)
            X_test = tfidfVectorizer.transform(feature_test)

            feature_names=tfidfVectorizer.get_feature_names()

            return X_train, X_test, feature_names

    def get_hashtagplusreply_features(self, tweets,tweet_test=None):


        tfidfVectorizer = CountVectorizer(ngram_range=(1,1),
                                          lowercase=True,
                                          binary=False,
                                          max_features=500000)

        if tweet_test is None:
            feature  = []
            for tweet in tweets:

                feature.append(tweet.hashtagplus)


            tfidfVectorizer = tfidfVectorizer.fit(feature)

            X = tfidfVectorizer.transform(feature)

            feature_names=tfidfVectorizer.get_feature_names()

            return X, feature_names
        else:
            feature  = []
            feature_test  = []
            for tweet in tweets:
                feature.append(tweet.hashtagplus)

            for tweet in tweet_test:
                feature_test.append(tweet.hashtagplus)


            tfidfVectorizer = tfidfVectorizer.fit(feature)

            X_train = tfidfVectorizer.transform(feature)
            X_test = tfidfVectorizer.transform(feature_test)

            feature_names=tfidfVectorizer.get_feature_names()

            return X_train, X_test, feature_names



    def get_mention_features(self, tweets,tweet_test=None):

        tfidfVectorizer = CountVectorizer(ngram_range=(1,1),
                                          lowercase=True,
                                          binary=False,
                                          max_features=500000)


        if tweet_test is None:
            feature  = []
            for tweet in tweets:

                feature.append((' '.join(re.findall(r"@(\w+)", tweet.tweet))))

            tfidfVectorizer = tfidfVectorizer.fit(feature)

            X = tfidfVectorizer.transform(feature)

            feature_names=tfidfVectorizer.get_feature_names()

            return X, feature_names
        else:
            feature  = []
            feature_test  = []
            for tweet in tweets:

                feature.append((' '.join(re.findall(r"@(\w+)", tweet.tweet))))

            for tweet in tweet_test:
                feature_test.append((' '.join(re.findall(r"@(\w+)", tweet.tweet))))


            tfidfVectorizer = tfidfVectorizer.fit(feature)

            X_train = tfidfVectorizer.transform(feature)
            X_test = tfidfVectorizer.transform(feature_test)

            feature_names=tfidfVectorizer.get_feature_names()

            return X_train, X_test, feature_names




    def get_mentionplus_features(self, tweets,tweet_test=None):


        tfidfVectorizer = CountVectorizer(ngram_range=(2,2),
                                          lowercase=True,
                                          binary=True,
                                          max_features=500000)

        if tweet_test is None:
            feature  = []
            for tweet in tweets:

                feature.append(tweet.mentionplus)


            tfidfVectorizer = tfidfVectorizer.fit(feature)

            X = tfidfVectorizer.transform(feature)

            feature_names=tfidfVectorizer.get_feature_names()

            return X, feature_names
        else:
            feature  = []
            feature_test  = []
            for tweet in tweets:
                feature.append(tweet.mentionplus)

            for tweet in tweet_test:
                feature_test.append(tweet.mentionplus)


            tfidfVectorizer = tfidfVectorizer.fit(feature)

            X_train = tfidfVectorizer.transform(feature)
            X_test = tfidfVectorizer.transform(feature_test)

            feature_names=tfidfVectorizer.get_feature_names()

            return X_train, X_test, feature_names


    def get_mentionplusreply_features(self, tweets,tweet_test=None):


        tfidfVectorizer = CountVectorizer(ngram_range=(2,2),
                                          lowercase=True,
                                          binary=True,
                                          max_features=500000)

        if tweet_test is None:
            feature  = []
            for tweet in tweets:

                feature.append(tweet.mentionplusreply)


            tfidfVectorizer = tfidfVectorizer.fit(feature)

            X = tfidfVectorizer.transform(feature)

            feature_names=tfidfVectorizer.get_feature_names()

            return X, feature_names
        else:
            feature  = []
            feature_test  = []
            for tweet in tweets:
                feature.append(tweet.mentionplusreply)

            for tweet in tweet_test:
                feature_test.append(tweet.mentionplusreply)


            tfidfVectorizer = tfidfVectorizer.fit(feature)

            X_train = tfidfVectorizer.transform(feature)
            X_test = tfidfVectorizer.transform(feature_test)

            feature_names=tfidfVectorizer.get_feature_names()

            return X_train, X_test, feature_names






    def get_communityfriend_features(self, tweets,tweet_test=None):

        tfidfVectorizer = CountVectorizer(ngram_range=(1,1),
                                          lowercase=True,
                                          binary=True,
                                          max_features=500000)


        if tweet_test is None:
            feature  = []
            for tweet in tweets:

                feature.append("featurecommunityfriend"+str(tweet.community_friend))

            tfidfVectorizer = tfidfVectorizer.fit(feature)

            X = tfidfVectorizer.transform(feature)

            feature_names=tfidfVectorizer.get_feature_names()

            return X, feature_names
        else:
            feature  = []
            feature_test  = []
            for tweet in tweets:
                feature.append("featurecommunityfriend"+str(tweet.community_friend))

            for tweet in tweet_test:
                feature_test.append("featurecommunityfriend"+str(tweet.community_friend))


            tfidfVectorizer = tfidfVectorizer.fit(feature)

            X_train = tfidfVectorizer.transform(feature)
            X_test = tfidfVectorizer.transform(feature_test)

            feature_names=tfidfVectorizer.get_feature_names()

            return X_train, X_test, feature_names



    def get_communityreply_features(self, tweets,tweet_test=None):

        tfidfVectorizer = CountVectorizer(ngram_range=(1,1),
                                          lowercase=True,
                                          binary=True,
                                          max_features=500000)


        if tweet_test is None:
            feature  = []
            for tweet in tweets:

                feature.append("featurecommunityreply1"+str(tweet.community_reply_1)+" "
                               "featurecommunityreply2"+str(tweet.community_reply_2)+" "
                               "featurecommunityreply3"+str(tweet.community_reply_3)+" "
                               "featurecommunityreply4"+str(tweet.community_reply_4)+" "
                               )

            tfidfVectorizer = tfidfVectorizer.fit(feature)

            X = tfidfVectorizer.transform(feature)

            feature_names=tfidfVectorizer.get_feature_names()

            return X, feature_names
        else:
            feature  = []
            feature_test  = []
            for tweet in tweets:
                feature.append("featurecommunityreply1"+str(tweet.community_reply_1)+" "
                               "featurecommunityreply2"+str(tweet.community_reply_2)+" "
                               "featurecommunityreply3"+str(tweet.community_reply_3)+" "
                               "featurecommunityreply4"+str(tweet.community_reply_4)+" "
                               )


            for tweet in tweet_test:
                feature_test.append("featurecommunityreply1"+str(tweet.community_reply_1)+" "
                               "featurecommunityreply2"+str(tweet.community_reply_2)+" "
                               "featurecommunityreply3"+str(tweet.community_reply_3)+" "
                               "featurecommunityreply4"+str(tweet.community_reply_4)+" "
                               )



            tfidfVectorizer = tfidfVectorizer.fit(feature)

            X_train = tfidfVectorizer.transform(feature)
            X_test = tfidfVectorizer.transform(feature_test)

            feature_names=tfidfVectorizer.get_feature_names()

            return X_train, X_test, feature_names


    def get_communityquote_features(self, tweets,tweet_test=None):

        tfidfVectorizer = CountVectorizer(ngram_range=(1,1),
                                          lowercase=True,
                                          binary=True,
                                          max_features=500000)


        if tweet_test is None:
            feature  = []
            for tweet in tweets:

                feature.append("featurecommunityquote1"+str(tweet.community_quote_1)+" "
                               "featurecommunityquote2"+str(tweet.community_quote_2)+" "
                               "featurecommunityquote3"+str(tweet.community_quote_3)+" "
                               "featurecommunityquote4"+str(tweet.community_quote_4)+" "
                               )

            tfidfVectorizer = tfidfVectorizer.fit(feature)

            X = tfidfVectorizer.transform(feature)

            feature_names=tfidfVectorizer.get_feature_names()

            return X, feature_names
        else:
            feature  = []
            feature_test  = []
            for tweet in tweets:
                feature.append("featurecommunityquote1"+str(tweet.community_quote_1)+" "
                               "featurecommunityquote2"+str(tweet.community_quote_2)+" "
                               "featurecommunityquote3"+str(tweet.community_quote_3)+" "
                               "featurecommunityquote4"+str(tweet.community_quote_4)+" "
                               )



            for tweet in tweet_test:
                feature_test.append("featurecommunityquote1"+str(tweet.community_quote_1)+" "
                               "featurecommunityquote2"+str(tweet.community_quote_2)+" "
                               "featurecommunityquote3"+str(tweet.community_quote_3)+" "
                               "featurecommunityquote4"+str(tweet.community_quote_4)+" "
                               )




            tfidfVectorizer = tfidfVectorizer.fit(feature)

            X_train = tfidfVectorizer.transform(feature)
            X_test = tfidfVectorizer.transform(feature_test)

            feature_names=tfidfVectorizer.get_feature_names()

            return X_train, X_test, feature_names


    def get_communityretweet_features(self, tweets,tweet_test=None):

        tfidfVectorizer = CountVectorizer(ngram_range=(1,1),
                                          lowercase=True,
                                          binary=True,
                                          max_features=500000)


        if tweet_test is None:
            feature  = []
            for tweet in tweets:

                feature.append("featurecommunityretweet1"+str(tweet.community_retweet_1)+" "
                               "featurecommunityretweet2"+str(tweet.community_retweet_2)+" "
                               "featurecommunityretweet3"+str(tweet.community_retweet_3)+" "
                               "featurecommunityretweet4"+str(tweet.community_retweet_4)+" "
                               )

            tfidfVectorizer = tfidfVectorizer.fit(feature)

            X = tfidfVectorizer.transform(feature)

            feature_names=tfidfVectorizer.get_feature_names()

            return X, feature_names
        else:
            feature  = []
            feature_test  = []
            for tweet in tweets:
                feature.append("featurecommunityretweet1"+str(tweet.community_retweet_1)+" "
                               "featurecommunityretweet2"+str(tweet.community_retweet_2)+" "
                               "featurecommunityretweet3"+str(tweet.community_retweet_3)+" "
                               "featurecommunityretweet4"+str(tweet.community_retweet_4)+" "
                               )


            for tweet in tweet_test:
                feature_test.append("featurecommunityretweet1"+str(tweet.community_retweet_1)+" "
                               "featurecommunityretweet2"+str(tweet.community_retweet_2)+" "
                               "featurecommunityretweet3"+str(tweet.community_retweet_3)+" "
                               "featurecommunityretweet4"+str(tweet.community_retweet_4)+" "
                               )



            tfidfVectorizer = tfidfVectorizer.fit(feature)

            X_train = tfidfVectorizer.transform(feature)
            X_test = tfidfVectorizer.transform(feature_test)

            feature_names=tfidfVectorizer.get_feature_names()

            return X_train, X_test, feature_names


#inizializer
def make_feature_manager():

    features_manager = Features_manager()

    return features_manager

