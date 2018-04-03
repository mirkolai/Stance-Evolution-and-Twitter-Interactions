
__author__ = 'mirko'
import numpy
from sklearn.svm.classes import SVC
import Features_manager
import Database_manager
from itertools import combinations
from sklearn.metrics.classification import precision_recall_fscore_support, accuracy_score
from sklearn.cross_validation import KFold
from collections import Counter


print("Task A - SVM")


for phase in [1,2,3,4]:

    database_manager=Database_manager.make_database_manager()
    feature_manager=Features_manager.make_feature_manager()

    tweets=numpy.array(database_manager.return_tweets(phase=phase))
    stance=numpy.array(feature_manager.get_stance(tweets))


    count = Counter(stance)
    print("Count most common",count.most_common())

    feature_names=numpy.array([
                   "BoW",
                   "hashtag",
                   "hashtagplus",
                   "hashtagplusreply",
                   "mention",
                   "mentionplus",
                   "mentionplusreply",

        "community_reply",
        "community_quote",
        "community_retweet",

        ])

    features_set=numpy.array([
    ["hashtagplus","hashtagplusreply", "mentionplus",
     "community_quote",
     "community_retweet",]
    ])


    #features_set=feature_names
    stuff = range(0, len(features_set) )
    X,feature_name_global,feature_index_global=feature_manager.create_feature_space(tweets,feature_names)

    #print(len(feature_name_global),len(feature_index_global),len(numpy.concatenate(feature_index_global)))
    max=0
    maxfeature=0
    for L in range(1, len(features_set)+1):
        for subset in combinations(stuff, L):
            fmacrosforcicle=[]

            kf = KFold(len(tweets),n_folds=5, shuffle=True, random_state=0)
            #print(subset)
            feature_filtered=numpy.concatenate(features_set[list(subset)])
            #print(feature_filtered)
            feature_index_filtered=numpy.array([list(feature_names).index(f) for f in feature_filtered])
            feature_index_filtered=numpy.concatenate(feature_index_global[list(feature_index_filtered)])
            #print(feature_name_global[feature_index_filtered])
            X_filter=X[:,feature_index_filtered]
            #print(feature_filtered,X.shape,X_filter.shape)
            predict=[]
            golden=[]
            for index_train, index_test in kf:

                X_train=X_filter[index_train]
                X_test=X_filter[index_test]

                clf = SVC(kernel='linear')

                clf.fit(X_train,stance[index_train])
                test_predict = clf.predict(X_test)
                predict=numpy.concatenate((predict,test_predict))
                golden=numpy.concatenate((golden,stance[index_test]))

            prec, recall, f, support = precision_recall_fscore_support(
            golden,
            predict,
            beta=1)

            accuracy = accuracy_score(
            golden,
            predict
            )





            print('"'+(' '.join(feature_filtered))+'"'+'\t'+str(((f[0]+f[1]+f[2])/3))+'\t'+
    str(((f[1]+f[2])/2))+'\t'+
    str(prec)+'\t'+str(recall)+'\t'+str(f)+'\n')

