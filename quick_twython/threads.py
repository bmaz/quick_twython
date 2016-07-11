import threading
from restApi import *

    filedir = "/home/bmazoyer/Documents/TwitterSea/Tests/"
    twitter = TwythonWrapper(CONSUMER_KEY, CONSUMER_SECRET, OAUTH_TOKEN, OAUTH_TOKEN_SECRET,filedir)
    tweets = twitter.past_search("#JeSuisPapillon")
    twitter.storeTweets(tweets, "test")
