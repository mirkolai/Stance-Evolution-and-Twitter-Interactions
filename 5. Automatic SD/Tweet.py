__author__ = 'mirko'

import re
import string
from Linguistic_resources import WORDLIST
import Database_manager

wordlist  = {"it": WORDLIST("it")}

class Tweet(object):

    id=""
    tweet=""
    retweet=""
    reply=""
    reply_to=""
    stance=""
    phase=""

    tweet_ulrs   = ""
    retweet_ulrs = ""
    reply_ulrs   = ""
    reply_to_ulrs= ""

    def __init__(self, database_manager, id, user_id, tweet, retweet, reply, reply_to,  stance, phase ):


        self.id       =id
        self.user_id  =user_id
        self.tweet    =re.sub(r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', ' URL ', tweet)
        self.retweet  =re.sub(r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', ' URL ', retweet)
        self.reply    =re.sub(r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', ' URL ', reply)
        self.reply_to =re.sub(r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', ' URL ', reply_to)
        self.stance   =stance
        self.phase    =phase
        self.hashtagplus = getHashtagplus(self.tweet,"it") + getHashtagplus(self.retweet,"it") +getHashtagplus(self.reply,"it") #+ getHashtagplus(self.reply_to,"it","replyto")
        self.mentionplus = getMentionplus(self.tweet,"it") + getMentionplus(self.retweet,"it") +getMentionplus(self.reply,"it") #+ getMentionplus(self.reply_to,"it","replyto")
        self.hashtagplusreply = getHashtagplus(self.reply_to,"it","replyto")
        self.mentionplusreply = getMentionplus(self.reply_to,"it","replyto")

        self.community_friend  = database_manager.return_community("friends",user_id)

        self.community_reply_1 = database_manager.return_community("reply",user_id,1)
        self.community_reply_2 = database_manager.return_community("reply",user_id,2)
        self.community_reply_3 = database_manager.return_community("reply",user_id,3)
        self.community_reply_4 = database_manager.return_community("reply",user_id,4)

        self.community_quote_1 = database_manager.return_community("quote",user_id,1)
        self.community_quote_2 = database_manager.return_community("quote",user_id,2)
        self.community_quote_3 = database_manager.return_community("quote",user_id,3)
        self.community_quote_4 = database_manager.return_community("quote",user_id,4)

        self.community_retweet_1 = database_manager.return_community("retweet",user_id,1)
        self.community_retweet_2 = database_manager.return_community("retweet",user_id,2)
        self.community_retweet_3 = database_manager.return_community("retweet",user_id,3)
        self.community_retweet_4 = database_manager.return_community("retweet",user_id,4)

        if False and len(re.findall(r"#(\w+)", self.tweet))>0:
            print(self.tweet.replace("\n",""))
            print(self.hashtagplus)



def getHashtagplus(text,language,header=""):
    hashtagsplus=""
    for hashtag in re.findall(r"#(\w+)", text):
        hashtagsplus += " "+header+wordlist[language].ParseHashtag(hashtag)

    #if len(hashtagsplus)<1:
    #    hashtagsplus=  " "+header+"NOHASHTAGPLUS"

    return hashtagsplus

def getMention(text):

    mentions = ' '.join(re.findall(r"@(\w+)", text))

    #if len(mentions)<1:
    #    mentions="NOMENTION"

    return mentions

def getMentionplus(text,language,header=""):
    mentionsplus=""
    for mention in re.findall(r"@(\w+)", text):
        mentionsplus += " "+header+wordlist[language].getMentionName(mention)

    include = set(string.ascii_letters)|set(" ")
    mentionsplus = ''.join(ch for ch in mentionsplus if ch in include)
    mentionsplus = re.sub(' {2,}',' ',mentionsplus)

    #if len(mentionsplus)<1:
    #    mentionsplus= " "+header+"NOMENTIONPLUS"

    return mentionsplus

def getMentionplusplus(text,language):
    mentionsplus=""
    for mention in re.findall(r"@(\w+)", text):
        mentionsplus += " "+wordlist[language].getMentionDescription(mention)

    include = set(string.ascii_letters)|set(" ")
    mentionsplus = ''.join(ch for ch in mentionsplus if ch in include)
    mentionsplus = re.sub(' {2,}',' ',mentionsplus)

    #if len(mentionsplus)<1:
    #    mentionsplus="NOMENTIONPLUSPLUS"

    return mentionsplus

def make_tweet(database_manager,id,user_id, tweet, retweet, reply, reply_to,  stance, phase ):

    tweet = Tweet(database_manager,id, user_id, tweet, retweet, reply, reply_to,  stance, phase )

    return tweet



if __name__ == '__main__':

    hashtagplus = getHashtagplus(" #votatesi #votosi che fate #ciao  #votatono","it")
    print(hashtagplus)