import threading
import logging
from restApi import *
from config import *



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
                search_result = self.twitter.past_search(queries[0], end_search)
            else:
                search_result = self.twitter.past_search(queries[0], end_search, min(t["id"] for t in tweets))
            tweets.extend(search_result)
        if search_result != []:
            self.twitter.storeTweets(search_result, filenames[0])

if __name__ == "__main__":
    filedir = "/home/bmazoyer/Documents/TwitterSea/Tests/"
    queries = ["Sagan", "#FRAPOR"]
    filenames = ["Sagan", "#FRAPOR"]

    while True:
        for i in range(len(ACCESS)):
            t = SearchThread(i)
            t.start()
            t.join()
            if i == 0:
                reset_time = t.twitter.time_limit("search")
            if end_search.is_set():
                queries = queries[1:]
                filenames = filenames[1:]
                if queries == []:
                    break
        if queries == []:
            break
        if reset_time - time.time()>0:
            minutes, seconds = divmod(reset_time - time.time(), 60)
            print("Sleeping for {}\'{}\"".format(round(minutes), round(seconds)))
            time.sleep(reset_time - time.time())
