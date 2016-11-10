from restApi import *
from config import *
from retrieveTweets import *
from datetime import datetime, timedelta

def hourly_search(filedir, function, queries, event, start_date=datetime.utcnow()-timedelta(days=9), hourly=True, timelapse=None):

    if isinstance(start_date, str):
        start_date = datetime.strptime(start_date, "%Y-%m-%dT%H:%M:%S")
    if hourly == True:
        t2 = start_date + timedelta(hours=1)
    else:
        t2 = start_date +timedelta(days=1)
    t1 = start_date
    tweet1 = retrieveHourlyTweet(None, round(time.mktime(t1.timetuple())*1000))

    if timelapse == None:
        timelapse = (datetime.utcnow() - start_date)
        if hourly == True:
            timelapse = timelapse.days*24 + timelapse.seconds//3600
        else:
            timelapse = timelapse.days
    print(start_date, t2)
    for i in range(timelapse + 1):
        tweet2 = retrieveHourlyTweet(round(time.mktime(t1.timetuple())*1000), round(time.mktime(t2.timetuple())*1000))
        if tweet2 != None:
            print(tweet1["_source"]["user_screen_name"], tweet1["_id"], tweet1["_source"]["created_at"])
            print(tweet2["_source"]["user_screen_name"], tweet2["_id"], tweet2["_source"]["created_at"])
            handle_limits_hourly(filedir, function, queries, event, tweet1["_id"], tweet2["_id"])
            t1 = t2
            tweet1 = tweet2
        if hourly == True:
            t2 = t2 + timedelta(hours=1)
        else:
            t2 = t2 + timedelta(days=1)

def handle_limits_hourly(filedir, function, queries, event, *args):
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
                query_result, queries = method(queries, event, query_result, *args)
                results[position].update(query_result)
                if queries == []:
                    return results
                remaining_requests = twitter.limits[function]["remaining"]
            if 'reset_time' in locals():
                reset_time = min(reset_time, twitter.limits[function]["reset"])
            else:
                reset_time = twitter.limits[function]["reset"]
        print("reset_time - time", reset_time - time.time())
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

def retrieve_once(function, key_nbr, *args):
    key = ACCESS[key_nbr]
    twitter = TwythonWrapper(
        key["consumer_key"],
        key["consumer_secret"],
        key["oauth_token"],
        key["oauth_token_secret"],
        filedir)
    method = getattr(twitter, function)
    return method(*args)

if __name__ == "__main__":
    filedir = "/home/bmazoyer/Documents/TwitterSea/News/"
    # trends = retrieve_once("get_trends", 1)
    # for trend in trends:
    #     print(trend['name'], trend['tweet_volume'])


    # results = []
    # # queries = ["Brexit"]
    event = { "date": "2016-10-14T17:21:20", "id": "afp.com-20161014T172120Z-TX-PAR-IZK65", "text": "35h Hanouna C8 CSA saisi Laurence Rossignol feministes" }
    queries = ["CSA agression sexuelle"]
    method = "plain_search"
    # # handle_limits(method, queries)
    handle_limits_hourly(filedir, method, queries, event)
    # # hourly_search(filedir, method, queries, event, "2016-09-06T07:00:00", False)
