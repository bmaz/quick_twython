from restApi import *
from config import *
from retrieveTweets import *
from datetime import datetime, timedelta

def hourly_search(function, queries, start_date=datetime.utcnow()-timedelta(days=9), timelapse=None):

    if isinstance(start_date, str):
        start_date = datetime.strptime(start_date, "%Y-%m-%dT%H:%M:%S")
    t2 = start_date + timedelta(hours=1)
    t1 = start_date
    tweet1 = retrieveHourlyTweet(None, round(time.mktime(t1.timetuple())*1000))

    if timelapse == None:
        timelapse = (datetime.utcnow() - start_date)
        timelapse = timelapse.days*24 + timelapse.seconds//3600

    for i in range(timelapse + 2):
        tweet2 = retrieveHourlyTweet(round(time.mktime(t1.timetuple())*1000), round(time.mktime(t2.timetuple())*1000))
        if tweet2 != None:
            print(tweet1["_source"]["user_screen_name"], tweet1["_id"], tweet1["_source"]["created_at"])
            print(tweet2["_source"]["user_screen_name"], tweet2["_id"], tweet2["_source"]["created_at"])
            handle_limits_hourly(function, queries, tweet2["_id"], tweet1["_id"])
            t1 = t2
            tweet1 = tweet2
        t2 = t2 + timedelta(hours=1)

def handle_limits_hourly(function, queries, *args):
    results = [set() for x in queries]

    while True:
        for i in range(len(ACCESS)):
            print("key :", i)
            key = ACCESS[i]
            twitter = TwythonWrapper(
                key["consumer_key"],
                key["consumer_secret"],
                key["oauth_token"],
                key["oauth_token_secret"],
                filedir)
            method = getattr(twitter,function)
            remaining_requests = twitter.limits[function]["remaining"]
            while remaining_requests > 0:
                position = len(results)-len(queries)
                query_result = results[position]
                query_result, queries = method(queries, query_result, *args)
                results[position].update(query_result)
                if queries == []:
                    return
                remaining_requests = remaining_requests - len(query_result)//100
            if 'reset_time' in locals():
                reset_time = min(reset_time, twitter.limits[function]["reset"])
            else:
                reset_time = twitter.limits[function]["reset"]
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
    filedir = "/home/bmazoyer/Documents/TwitterSea/SaintEtienneDuRouvray/"
    results = []
    # queries = ["Brexit"]
    queries = ["CoucouHibou","AbdelMalikPetitjean"]
    method = "plain_search"
    # handle_limits(method, queries)
    handle_limits_hourly(method, queries)
    # hourly_search(method, queries, "2016-07-31T03:00:00")
