import threading
from streamingAPI import *
from retrieveTweets import storeTweetsWithTag
from config import *
import logging
import time

class StreamThread(threading.Thread):
    def __init__(self, threadNum, track = [], lang="fr"):
        threading.Thread.__init__(self)
        key = ACCESS[threadNum]
        self.threadNum = threadNum
        self.track = track
        self.lang = lang
        self.twitter =  sampleStreamer(
            key["consumer_key"],
            key["consumer_secret"],
            key["oauth_token"],
            key["oauth_token_secret"],
            filedir+"_"+str(lang)+"_",
            filebreak)

    def run(self):
        self.twitter.filter(self.track, self.lang)
        e.set()
        print("sample ended")


class StoreThread(threading.Thread):
    def __init__(self, streamer):
        threading.Thread.__init__(self)
        self.streamer = streamer


    def run(self):
        time.sleep(10)
        while True:
            tweets = self.streamer.tweets
            storeTweetsWithTag(tweets, "_filter")
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
    filedir = "/rex/store1/crp/tweets_beatrice/Filter/_sample"
    filebreak = 1000
    track = ["rt","https","t.co","de","la","le","les","et","à","pas","je","un",
    "que","en","est","pour","des","a","une","qui","tu","il","sur","est","du","mais",
    "ça","on","dans","ai","ce","avec","plus","vous","quand","au","me","moi","si",
    "elle","ma","fait","mon","c","se","trop","tout","ne","bien","comme","faire","sa",
    "va","suis","même","par","te","toi","ou","y","sont","vie","son","quoi","lui",
    "nous","2","ils","ta","mdr","t","rien","cette","bon","être","mes","non","3",
    "gens","aussi","jamais","mort","es","ton","1","là","aime","ca","fais","tous",
    "twitter","merci","temps","dit","ans","oui","après","jsuis","vidéo","aux",
    "faut","vraiment","comment","monde","dire","encore","voir"]
    e = threading.Event()
    s = StreamThread(1, track, )
    t = StoreThread(s.twitter)
    s.start()
    t.start()
    input("End streaming ? \n")
    s.twitter.do_continue = False
