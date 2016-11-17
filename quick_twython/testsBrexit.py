# -*- coding: utf-8 -*-
from datetime import *
import time
from elasticsearch import Elasticsearch, helpers
import json
import re
from os import listdir, path
import glob


def storeBrexit(tweets):
    tweets_not_created = []
    es = Elasticsearch()
    settings = {
        "mappings": {
            "urls": {
                "properties": {
                    "tags": {
                        "type": "string",
                        "index" : "not_analyzed"
                    }
                }
            }
        }
    }

    to_update = (
    {
    '_op_type': 'delete',
    '_type':'news',
    '_index':'tweets_index',
    '_id': tweet[2]["id"]
    # 'script': "if (ctx._source.containsKey(\"tags\")) {ctx._source.tags = (ctx._source.tags + query).unique()} else {ctx._source.tags = [query]}",
    # # 'script' : "if (ctx._source.containsKey(\"tags\")) {ctx._source.tags+= query} else {ctx._source.tags = [query]}",
    # 'params': {
    #     'query': query
    #     },
    # 'upsert': {
    #     'text': tweet[2]["text"],
    #     'user_screen_name': tweet[2]["user"]["screen_name"],
    #     'hashtags': tweet[2]["entities"]["hashtags"],
    #     'created_at' : datetime.strptime(
    #     tweet[2]["created_at"],
    #     "%a %b %d %H:%M:%S +0000 %Y").strftime("%Y-%m-%dT%H:%M:%S"
    #     ),
    #     'collection_date' : tweet[0],
    #     'favorite_count' : tweet[2]["favorite_count"],
    #     'retweet_count' : tweet[2]["retweet_count"],
    #     'user_mentions' :[user["screen_name"] for user in tweet[2]["entities"]["user_mentions"]],
    #     'in_reply_to_screen_name' : tweet[2]["in_reply_to_screen_name"],
    #     'author_quoted' : tweet[2]["quoted_status"]["user"]["screen_name"] if "quoted_status" in tweet[2].keys() else None,
    #     'is_retweet' : True if "retweeted_status" in tweet[2].keys() else False,
    #     'tweet_retweeted' :  tweet[2]["retweeted_status"]["id"] if "retweeted_status" in tweet[2].keys() else None,
    #     'author_retweeted' : tweet[2]["retweeted_status"]["user"]["screen_name"] if "retweeted_status" in tweet[2].keys() else None,
    #     'tags' : [query]
        # }
    }
          for tweet in tweets)
    try:
        res = helpers.bulk(es,to_update,True)
        print(res)
    except helpers.BulkIndexError as error:
        res = error.errors
        for r in res:
            tweets_not_created.append(r['delete']['_id'])
        print("tweets not created", tweets_not_created)

    
dirpath = "/home/bmazoyer/Documents/TwitterSea/News/afp.com-20161115T181832Z-TX-PAR-LEL49_Hollande veut prolonger etat urgence jusqu presidentielle"

for filename in glob.glob(dirpath + "/Hollande*"):
    text = []
    print(filename)
    with open(filename, "r+", encoding="utf-8") as stream:
        file = stream.readlines()
        for r in file:
            try:
                tweet = json.loads(r)
                if tweet[1] == "-1":
                    tweet[2]["id"] = tweet[2]["delete"]["status"]["id"]
                    tweet[2]["timestamp_ms"] = tweet[2]["delete"]["timestamp_ms"]
                # if "lang" in tweet[2] and tweet[2]["lang"] == "en":
                text.append(tweet)
            except Exception as error:
                    pass
                    print(error)
        for i in [n*100 for n in range(round(len(text)/100))]:
            storeBrexit(text[i:i+100])
