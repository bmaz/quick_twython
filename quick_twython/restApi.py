# -*- coding: utf-8 -*-
from config import *
from retrieveTweets import storeTweetsWithTag
import time
from datetime import datetime, timedelta
from twython import Twython, TwythonError, TwythonRateLimitError
import json
import os
# import threading

class TwythonWrapper(Twython):
    """
    Wrapper for twitter REST APIs calls
    """
    def __init__(self, app_key, app_secret, oauth_token, oauth_token_secret, filedir):
        self.filedir = filedir
        Twython.__init__(self, app_key, app_secret, oauth_token, oauth_token_secret)
        self.limits = {}
        self.update_limits()

    def update_limits(self):
        limits = self.limits
        try:
            update = self.get_application_rate_limit_status()
            limits["plain_search"] = update["resources"]["search"]["/search/tweets"]
        except TwythonRateLimitError as error:
            print("update limits reached")
            if "plain_search" in limits:
                limits["plain_search"]["remaining"] = 0
            else:
                limits["plain_search"] = {}
                limits["plain_search"]["remaining"] = 0
                limits["plain_search"]["reset"] = time.time() + 15*60

        self.limits = limits

    # def time_limit(self, method):
    #     if method == self.plain_search:
    #         reset_time = float(
    #             self.get_application_rate_limit_status(resources = "search")["resources"]["search"]["/search/tweets"]["reset"]
    #         )
    #     return reset_time
    #
    # def volume_limit(self, method):
    #     if method == self.plain_search:
    #         remaining = self.get_application_rate_limit_status(resources = "search")["resources"]["search"]["/search/tweets"]["remaining"]
    #     return remaining


    def on_error(self, error):
        print("Some other error occured, taking a break for half a minute: " + str(error))
        time.sleep(30)

    def get_trends(self):
        return self.get_place_trends(id=23424819)[0]['trends']

    def plain_search(self, queries, event, ids=set(), since_id=None, max_id=None, until=None):
        """
        Returns a collection of relevant Tweets matching a specified query.
        Returns a list of tweets when twitter API time limit is reached
        or when no more tweets can be collected.
        Docs: https://dev.twitter.com/docs/api/1.1/get/search/tweets
        """
        print("Start searching for query " + queries[0])
        results = []

        for i in range(0,MAX_ATTEMPTS):
            if(COUNT_OF_TWEETS_TO_BE_FETCHED < len(results)):
                print("more than ", COUNT_OF_TWEETS_TO_BE_FETCHED, " tweets")
                queries = queries[1:]
                break # we got COUNT_OF_TWEETS_TO_BE_FETCHED tweets... !!

            #----------------------------------------------------------------#
            # STEP 1: Query Twitter
            # STEP 2: Save the returned tweets
            # STEP 3: Get the next max_id
            #----------------------------------------------------------------#
            try :
                # STEP 1: Query Twitter
                if ids == set():
                    result = self.search(q=queries[0], lang =LANG, max_id=max_id, count='100', since_id=since_id, until=until)
                else:
                    result = self.search(q=queries[0], lang =LANG, max_id=min(ids)-1, count='100', since_id=since_id, until=until)

                for t in result['statuses']:
                    # print(t["created_at"], t["id"])
                    if t["id"] not in ids:
                        ids.add(t["id"])
                        results.append(t)
                if result['statuses'] != []:
                    print(results[-1]["created_at"], results[-1]["id"])
                else:
                    queries = self.beforeReturn(queries, i+1)
                    break

                # STEP 3: Check if next results are coming. If not, end search
                try:
                    next_results_url_params = result['search_metadata']['next_results']
                except KeyError:
                    if results != []:
                        storeTweets(self.filedir, results, queries[0], event)
                    queries = self.beforeReturn(queries, i+1)
                    break
                except Exception as error:
                    print(error)

            except TwythonRateLimitError as error:
                print(error)
                self.update_limits()
                if results != []:
                    storeTweets(self.filedir, results, queries[0], event)
                break
            except TwythonError as error:
                self.on_error(error)
                continue

        return ids, queries

    def beforeReturn(self, queries, attempts):
        self.limits["plain_search"]["remaining"] = max(self.limits["plain_search"]["remaining"] - attempts, 0)
        print("Over: attempts: ", attempts, "remaining: ", self.limits["plain_search"]["remaining"])
        return queries[1:]

def storeTweets(filedir, tweets, tag="", event={}):
    """
    Store tweets in a file called "tag_nb-of-tweets_date-and-time.json"
    Tweets are separated by "\n"
    Also store tweets in elasticsearch with tag added to the queries field
    :param tweets: a list of tweets to store
    :param tag: a tag for the store file
    """
    storeTweetsWithTag(tweets, tag, event)
    if "id" in event:
        filedir = filedir + event["id"] + "/"
    if os.path.exists(filedir) == False:
        os.makedirs(filedir)
    tag = tag.replace("/","§")
    filename = filedir + tag + "_" + str(len(tweets)) + "_" + time.strftime("%Y-%m-%dT%H_%M_%S", time.gmtime()) + ".json"
    outputfile = open(filename,"a+",encoding='utf-8')
    for tweet in tweets:
        obj = []
        obj.append(round(time.time()))
        obj.append(tweet["id"])
        obj.append(tweet)
        json.dump(obj, outputfile)
        outputfile.write("\n")
    outputfile.close()


if __name__ == "__main__":
    filedir = "/home/bmazoyer/Documents/TwitterSea/Tests/"
    key = ACCESS[0]
    twitter = TwythonWrapper(
        key["consumer_key"],
        key["consumer_secret"],
        key["oauth_token"],
        key["oauth_token_secret"],
        filedir)
    tweets = twitter.plain_search(["#JeSuisPapillon"])
