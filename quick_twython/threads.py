import threading
from streamingAPI import *
from retrieveTweets import storeTweetsWithTag
from config import *
import logging
import time

class StreamThread(threading.Thread):
    def __init__(self, threadNum, lang=None):
        threading.Thread.__init__(self)
        key = ACCESS[threadNum]
        self.threadNum = threadNum
        self.lang = lang
        self.twitter =  sampleStreamer(
            key["consumer_key"],
            key["consumer_secret"],
            key["oauth_token"],
            key["oauth_token_secret"],
            filedir+"_"+str(lang)+"_",
            filebreak)

    def run(self):
        self.twitter.sample(self.lang)
        e.set()
        print("sample ended")


class StoreThread(threading.Thread):
    def __init__(self, streamer):
        threading.Thread.__init__(self)
        self.streamer = streamer


    def run(self):
        time.sleep(2)
        while True:
            tweets = self.streamer.tweets
            storeTweetsWithTag(tweets, "_sample")
            for data in tweets:
                obj = []
                obj.append(round(time.time()))

                if("id" in data):
                    obj.append(data["id"])
                else:
                    obj.append("-1")
                obj.append(data)
                json.dump(obj,self.streamer.outputfile)
                self.streamer.outputfile.write("\n")
                self.streamer.counter+=1
                if(self.streamer.counter>self.streamer.filebreak):
                    print(self.streamer.counter)
                    filename=self.streamer.filedir + time.strftime("%Y-%m-%dT%H_%M_%S", time.gmtime()) +".json"
                    self.streamer.outputfile.close()
                    self.streamer.outputfile =  open(filename,"a+",encoding='utf-8')
                    self.streamer.counter = 0
                self.streamer.tweets.remove(data)
            if e.isSet() and self.streamer.tweets == []:
                break
        print(self.streamer.tweets)

if __name__ == "__main__":
    filedir = "/rex/store1/crp/tweets_beatrice/Sample/_sample"
    filebreak = 1000
    e = threading.Event()
    s = StreamThread(2)
    t = StoreThread(s.twitter)
    s.start()
    t.start()
    input("End streaming ? \n")
    s.twitter.do_continue = False
