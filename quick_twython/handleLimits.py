from restApi import *
from config import *
from retrieveTweets import *
from datetime import datetime, timedelta

def hourly_search(queries):
    today = datetime.utcnow()
    week_ago = today - timedelta(days=8) + timedelta(hours= 11)
    t2 = week_ago + timedelta(hours=1)
    t1 = week_ago
    tweet1 = retrieveHourlyTweet(None, round(time.mktime(t1.timetuple())*1000))
    print(tweet1["_source"]["user_screen_name"], tweet1["_id"])

    for i in range(168):
        print(t1, t2)
        tweet2 = retrieveHourlyTweet(round(time.mktime(t1.timetuple())*1000), round(time.mktime(t2.timetuple())*1000))
        if tweet2 != None:
            print(tweet2["_source"]["user_screen_name"], tweet2["_id"])
            handle_limits_hourly(queries, tweet1["_id"], tweet2["_id"])
            t1 = t1 + timedelta(hours=1)
        t2 = t2 + timedelta(hours=1)

def handle_limits_hourly(queries, id1, id2):
    results = [[] for x in queries]
    while True:
        for i in range(len(ACCESS)):
            key = ACCESS[i]
            twitter = TwythonWrapper(
                key["consumer_key"],
                key["consumer_secret"],
                key["oauth_token"],
                key["oauth_token_secret"],
                filedir)
            method = getattr(twitter,"plain_search")
            remaining_requests = twitter.volume_limit(method)
            while remaining_requests > 0:
                position = len(results)-len(queries)
                query_result = results[position]
                query_result, queries = method(queries, query_result, id2, id1)
                results[position].extend(query_result)
                if queries == []:
                    return
                    # return results
                remaining_requests = twitter.volume_limit(method)
            if i == 0:
                reset_time = twitter.time_limit(method)

        if reset_time - time.time()>0:
            minutes, seconds = divmod(reset_time - time.time(), 60)
            print("Sleeping for {}\'{}\"".format(round(minutes), round(seconds)))
            time.sleep(reset_time - time.time())

def handle_limits(function, queries):
    results = [[] for x in queries]

    while True:
        for i in range(len(ACCESS)):
            key = ACCESS[i]
            twitter = TwythonWrapper(
                key["consumer_key"],
                key["consumer_secret"],
                key["oauth_token"],
                key["oauth_token_secret"],
                filedir)
            method = getattr(twitter,function)
            remaining_requests = twitter.volume_limit(method)
            while remaining_requests > 0:
                position = len(results)-len(queries)
                query_result = results[position]
                query_result, queries = method(queries, query_result)
                results[position].extend(query_result)
                if queries == []:
                    return results
                remaining_requests = twitter.volume_limit(method)
            if i == 0:
                reset_time = twitter.time_limit(method)

        if reset_time - time.time()>0:
            minutes, seconds = divmod(reset_time - time.time(), 60)
            print("Sleeping for {}\'{}\"".format(round(minutes), round(seconds)))
            time.sleep(reset_time - time.time())

if __name__ == "__main__":
    filedir = "/home/bmazoyer/Documents/TwitterSea/Tests/"
    results = []
    queries = ["Nice", "#Nice06", "#NiceAttentat", "Promenade des Anglais", "Camion prom"]
    # method = "hourly_search"
    # test = handle_limits(method, queries)
    hourly_search(queries)
