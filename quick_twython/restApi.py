# -*- coding: utf-8 -*-
from config import *
import time
from datetime import datetime, timedelta
from twython import Twython, TwythonError, TwythonRateLimitError
import json
# import threading

class TwythonWrapper(Twython):
    """
    Wrapper for twitter REST APIs calls
    """
    def __init__(self, app_key, app_secret, oauth_token, oauth_token_secret, filedir):
        self.filedir = filedir
        Twython.__init__(self, app_key, app_secret, oauth_token, oauth_token_secret)
        self.reset_time = float(
            self.get_application_rate_limit_status()["resources"]["search"]["/search/tweets"]["reset"]
        )

    def time_limit(self, method):
        if method == self.plain_search:
            reset_time = float(
                self.get_application_rate_limit_status(resources = "search")["resources"]["search"]["/search/tweets"]["reset"]
            )
        return reset_time

    def volume_limit(self, method):
        if method == self.plain_search:
            remaining = self.get_application_rate_limit_status(resources = "search")["resources"]["search"]["/search/tweets"]["remaining"]
        return remaining


    def on_error(self, error):
        print("Some other error occured, taking a break for half a minute: " + str(error))
        time.sleep(30)

    def plain_search(self, queries, results=[], max_id=None, since_id=None, until=None):
        """
        Returns a collection of relevant Tweets matching a specified query.
        Returns a list of tweets when twitter API time limit is reached
        or when no more tweets can be collected.
        Docs: https://dev.twitter.com/docs/api/1.1/get/search/tweets
        """
        print("Start searching for query " + queries[0])

        for i in range(0,MAX_ATTEMPTS):
            if results == []:
                all_ids = set()
                print("max_id: ", max_id, "min_id: ", since_id)
            else:
                all_ids = set(t["id"] for t in results)
                print("max_id: ", min(t["id"] for t in results), "min_id: ", since_id)
            if i < 5:
                print(i)
            elif i % 10 == 0:
                print(i)
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
                if results == []:
                    result = self.search(q=queries[0], lang =LANG, max_id=max_id, count='100', since_id=since_id, until=until)
                else:
                    result = self.search(q=queries[0], lang =LANG, max_id=min(t["id"] for t in results), count='100', since_id=since_id, until=until)

                print("uniques: ", len(set(t["id"] for t in result['statuses'])),
                "min_found: ", min(t["id"] for t in result['statuses']),
                "max_found: ", max(t["id"] for t in result['statuses']))
                ids = set()
                for t in result['statuses']:
                    if t["id"] not in ids:
                        ids.add(t["id"])
                    else:
                        result['statuses'].remove(t)

                #STEP 2: Save the returned tweets
                results.extend(result['statuses'])

                # STEP 3: Check if next results are coming. If not, end search
                try:
                    next_results_url_params = result['search_metadata']['next_results']
                except KeyError:
                    if results != []:
                        storeTweets(self.filedir, results, queries[0])
                    queries = queries[1:]
                    print("Over")
                    break

            except TwythonRateLimitError as error:
                if results != []:
                    storeTweets(self.filedir, results, queries[0])
                break
            except TwythonError as error:
                self.on_error(error)
                continue
        query_result = [min((t for t in results), key= lambda x: x["id"])] if results != [] else []
        return query_result, queries

def storeTweets(filedir, tweets, name=""):
    """
    Store tweets in a file called "name_nb-of-tweets_date-and-time.json"
    Tweets are separated by "\n"
    :param tweets: a list of tweets to store
    :param name: a name for the store file
    """
    filename = filedir + name + "_" + str(len(tweets)) + "_" + time.strftime("%Y-%m-%dT%H_%M_%S", time.gmtime()) +".json"
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
