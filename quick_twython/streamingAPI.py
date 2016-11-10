# -*- coding: utf-8 -*-
"""
Created on Tue May 17 17:31:33 2016

@author: zpehlivan
"""


import _thread
from twython import Twython, TwythonError, TwythonRateLimitError,TwythonStreamer
import time
import json
import sys
import yaml
import requests
from config import *

MAX_ATTEMPTS = 100
# Max Number of tweets per 15 minutes
COUNT_OF_TWEETS_TO_BE_FETCHED = 1000



class sampleStreamer(TwythonStreamer):
    """
    Retrieve data from the Twitter Streaming API.

    The streaming API requires
    `OAuth 1.0 <http://en.wikipedia.org/wiki/OAuth>`_ authentication.
    """
    def __init__(self, app_key, app_secret, oauth_token, oauth_token_secret,filedir,filebreak):

        filename=filedir + time.strftime("%Y-%m-%dT%H_%M_%S", time.gmtime()) +".json"
        self.handler = None
        self.oauth_token = oauth_token
        self.filedir = filedir
        self.counter = 0
        self.do_continue = True
        self.outputfile =  open(filename,"a+",encoding='utf-8')
        self.filebreak = filebreak
        self.tweets = []
        TwythonStreamer.__init__(self, app_key, app_secret, oauth_token,
                                 oauth_token_secret)

    def on_success(self, data):
        """
        :param data: response from Twitter API
        """
        self.tweets.append(data)
        if self.do_continue == False:
            print("disconnect")
            self.disconnect()


    def on_error(self, status_code, data):
        """
        :param status_code: The status code returned by the Twitter API
        :param data: The response from Twitter API

        """
        print(status_code)
        print(data)

    def sample(self, lang = None):
        """
        Wrapper for 'statuses / sample' API call
        """
        while self.do_continue:

            # Stream in an endless loop until limit is reached. See twython
            # issue 288: https://github.com/ryanmcgrath/twython/issues/288
            # colditzjb commented on 9 Dec 2014

            try:
                self.statuses.sample(language=lang)
            except requests.exceptions.ChunkedEncodingError as e:
                if e is not None:
                    print("Error (stream will continue): {0}".format(e))
            except Exception as error:
                print("Other exception :", error)
                self.disconnect()
                continue


    def filter(self, track='', lang='fr'):
        """
        Wrapper for 'statuses / filter' API call
        """

        while self.do_continue:
            #Stream in an endless loop until limit is reached

            try:
                # if track == '' and follow == '':
                #     msg = "Please supply a value for 'track', 'follow'"
                #     raise ValueError(msg)
                self.statuses.filter(track=track, language=lang)
            except requests.exceptions.ChunkedEncodingError as e:
                if e is not None:
                    print("Error (stream will continue): {0}".format(e))
                continue
            except Exception as error:
                print("Other exception :", error)
                self.disconnect()
                print("sleep : 20 sec")
                time.sleep(20)
                continue


def sampleapi(key_num,filedir,filebreak, lang=None):
     print("Starting sampleapi")
     key = ACCESS[key_num]
     ts = sampleStreamer(key["consumer_key"],
     key["consumer_secret"],
     key["oauth_token"],
     key["oauth_token_secret"],
     filedir+"_sample",
     filebreak)
     ts.sample(lang);


def streamingapifortrends(filedir,filebreak) :

    print("Starting search ny trends")
    twitter = Twython(seb_CONSUMER_KEY, seb_CONSUMER_SECRET, seb_OAUTH_TOKEN, seb_OAUTH_TOKEN_SECRET)
    trends = twitter.get_place_trends(id=1)
#    usa 23424977
#keeping trends file
    trendsfile = open(filedir+"trends.jsons","a+",encoding='utf-8')
    json.dump(trends,trendsfile)
    trendsfile.close()


#getting queries
    queries = []
    for t in trends[0]["trends"]:
        queries.append(t["name"])

    print(queries)
    twitter = sampleStreamer(seb_CONSUMER_KEY, seb_CONSUMER_SECRET, seb_OAUTH_TOKEN, seb_OAUTH_TOKEN_SECRET,filedir+"_search" ,filebreak)
    twitter.filter(track=queries);


if __name__ == "__main__":
    filedir = "/home/bmazoyer/Documents/TwitterSea/Stream/"
    filebreak = 1000
#    filedir = sys.argv[1]
#    filebreak = sys.argv[2]

    # _thread.start_new_thread(streamingapifortrends, (filedir,filebreak,))
    _thread.start_new_thread(sampleapi, (1, filedir,filebreak,"en"))
    _thread.start_new_thread(sampleapi, (0, filedir,filebreak))
    time.sleep(60*60)
    ts.do_continue = False
