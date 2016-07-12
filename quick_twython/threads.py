import threading
import logging
from restApi import *
from config import *

filedir = "/home/bmazoyer/Documents/TwitterSea/Tests/"

# logging.basicConfig(level=logging.DEBUG,format='[%(levelname)s] (%(threadName)-10s) %(message)s')

query = "Gaza"
nom_du_fichier = "Gaza"
tweets = []
lock = threading.RLock()

class SearchThread(threading.Thread):
    def __init__(self, threadNum):
        threading.Thread.__init__(self)
        key = ACCESS[threadNum]
        self.threadNum = threadNum
        self.twitter =  TwythonWrapper(
            key["consumer_key"],
            key["consumer_secret"],
            key["oauth_token"],
            key["oauth_token_secret"],
            filedir)

    def run(self):
        with lock:
            print("et c'est parti pour le thread ", self.threadNum)
            if tweets == []:
                result = self.twitter.past_search(query)
            else:
                result = self.twitter.past_search(query, min(t["id"] for t in tweets))
            tweets.extend(result)
        self.twitter.storeTweets(result, nom_du_fichier)

while True:
    for i in range(len(ACCESS)):
        t = SearchThread(i)
        t.start()
        if i == 0:
            reset_time = t.twitter.on_rate_limit_error("search")

    if reset_time - time.time()>0:
        print("prochain départ dans :", reset_time - time.time())
        time.sleep(reset_time - time.time())
    else:
        print(reset_time - time.time())
