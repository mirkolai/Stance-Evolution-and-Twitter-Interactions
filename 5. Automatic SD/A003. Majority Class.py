__author__ = 'mirko'
from collections import Counter
import numpy
import Features_manager
import Database_manager
from sklearn.metrics.classification import precision_recall_fscore_support, accuracy_score
from sklearn.cross_validation import KFold


database_manager=Database_manager.make_database_manager()
feature_manager=Features_manager.make_feature_manager()

tweets=numpy.array(database_manager.return_tweets())
stance=numpy.array(feature_manager.get_stance(tweets))


count = Counter(stance)
print(count.most_common())
majority_class=count.most_common()[0][0]
test_predict = [majority_class]*len(stance)

prec, recall, f, support = precision_recall_fscore_support(
stance,
test_predict,
beta=1)

accuracy = accuracy_score(
stance,
test_predict
)


print("f:",(f))
print("p:",(prec))
print("r:",(recall))

print('"MClass"'+'\t'+str(((f[1]+f[2])/2))+'\t'+str(((f[0]+f[1]+f[2])/3))+'\n')
