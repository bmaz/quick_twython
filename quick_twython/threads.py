import threading
import logging
from restApi import *
from config import *

filedir = "/home/bmazoyer/Documents/TwitterSea/Tests/"

# logging.basicConfig(level=logging.DEBUG,format='[%(levelname)s] (%(threadName)-10s) %(message)s')

query = "#Top14"
nom_du_fichier = "Top14"
tweets = []
lock = threading.RLock()
end_search = threading.Event()

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
            print("Account: ", self.threadNum)
            if tweets == []:
                search_result = self.twitter.past_search(query, end_search)
            else:
                search_result = self.twitter.past_search(query, end_search, min(t["id"] for t in tweets))
            tweets.extend(search_result)
        if search_result != []:
            self.twitter.storeTweets(search_result, nom_du_fichier)

while True:
    for i in range(len(ACCESS)):
        t = SearchThread(i)
        t.start()
        t.join()
        if i == 0:
            reset_time = t.twitter.time_limit("search")
        if end_search.is_set():
            break
    if end_search.is_set():
        break
    if reset_time - time.time()>0:
        minutes, seconds = divmod(reset_time - time.time(), 60)
        print("Sleeping for {}\'{}\"".format(round(minutes), round(seconds)))
        time.sleep(reset_time - time.time())
    else:
        print(reset_time - time.time())
