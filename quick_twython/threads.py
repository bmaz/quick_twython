import logging
from restApi import *
from config import *

def start_thread(twitter, method):
    if twitter.volume_limit(method) > 0:
        global results, queries
        results, queries = method(queries, results)

if __name__ == "__main__":
    filedir = "/home/bmazoyer/Documents/TwitterSea/Tests/"
    results = []
    queries = ["Sagan", "Proust"]


    while True:
        for i in range(len(ACCESS)):
            key = ACCESS[i]
            twitter = TwythonWrapper(
                key["consumer_key"],
                key["consumer_secret"],
                key["oauth_token"],
                key["oauth_token_secret"],
                filedir)
            method = twitter.plain_search
            remaining_requests = twitter.volume_limit(method)
            while remaining_requests > 0:
                results, queries = method(queries, results)
                if queries == []:
                    break
                remaining_requests = twitter.volume_limit(method)
            if i == 0:
                reset_time = twitter.time_limit(method)
            if queries == []:
                break

        if queries == []:
            break
        if reset_time - time.time()>0:
            minutes, seconds = divmod(reset_time - time.time(), 60)
            print("Sleeping for {}\'{}\"".format(round(minutes), round(seconds)))
            time.sleep(reset_time - time.time())
