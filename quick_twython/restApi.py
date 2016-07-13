# -*- coding: utf-8 -*-
from config import *
import time
from twython import Twython, TwythonError, TwythonRateLimitError
import json
import threading

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
        if method == "search":
            reset_time = float(
                self.get_application_rate_limit_status()["resources"]["search"]["/search/tweets"]["reset"]
            )
        return reset_time


    def on_error(self, error):
        print("Some other error occured, taking a break for half a minute: " + str(error))
        time.sleep(30)


    def storeTweets(self, tweets, name=""):
        """
        Store tweets in a file called "name_nb-of-tweets_date-and-time.json"
        Tweets are separated by "\n"
        :param tweets: a list of tweets to store
        :param name: a name for the store file
        """
        filename = self.filedir + name + "_" + str(len(tweets)) + "_" + time.strftime("%Y-%m-%dT%H_%M_%S", time.gmtime()) +".json"
        outputfile = open(filename,"a+",encoding='utf-8')
        for tweet in tweets:
            obj = []
            obj.append(round(time.time()))
            obj.append(tweet["id"])
            obj.append(tweet)
            json.dump(obj, outputfile)
            outputfile.write("\n")
        outputfile.close()



    def past_search(self, query, end_search = threading.Event(), next_max_id=None, since_id=None, until=None):
        """
        Returns a collection of relevant Tweets matching a specified query.
        Returns a list of tweets when twitter API time limit is reached
        or when no more tweets can be collected.
        When no more tweets can be collected, set end_search = threading.Event()
        Docs: https://dev.twitter.com/docs/api/1.1/get/search/tweets
        """
        tweets = []
        print("Start searching for query " + query)

        for i in range(0,MAX_ATTEMPTS):
            if i < 5:
                print(i)
            elif i % 10 == 0:
                print(i)
            if(COUNT_OF_TWEETS_TO_BE_FETCHED < len(tweets)):
                print("more than ", COUNT_OF_TWEETS_TO_BE_FETCHED, " tweets")
                break # we got COUNT_OF_TWEETS_TO_BE_FETCHED tweets... !!

            #----------------------------------------------------------------#
            # STEP 1: Query Twitter
            # STEP 2: Save the returned tweets
            # STEP 3: Get the next max_id
            #----------------------------------------------------------------#
            try :
                # STEP 1: Query Twitter
                # After the first call we should have max_id from result of previous call. Pass it in query.
                results = self.search(q=query, lang =LANG, max_id=next_max_id, count='100', since_id=since_id, until=until)

                # STEP 2: Save the returned tweets
                for result in results['statuses']:
                    tweets.append(result)


                # STEP 3: Get the next max_id
                try:
                    # Parse the data returned to get max_id to be passed in consequent call.
                    next_results_url_params = results['search_metadata']['next_results']
                    next_max_id = next_results_url_params.split('max_id=')[1].split('&')[0]

                except KeyError:
                    print("Over")
                    end_search.set()
                    break

            except TwythonRateLimitError as error:
                break
            except TwythonError as error:
                self.on_error(error)
                continue

        return tweets

if __name__ == "__main__":
    filedir = "/home/bmazoyer/Documents/TwitterSea/Tests/"
    twitter = TwythonWrapper(CONSUMER_KEY, CONSUMER_SECRET, OAUTH_TOKEN, OAUTH_TOKEN_SECRET,filedir)
    tweets = twitter.past_search("#JeSuisPapillon")
    twitter.storeTweets(tweets, "test")
