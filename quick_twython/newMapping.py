from elasticsearch import Elasticsearch, helpers
import os
import json
import re
from datetime import *

es = Elasticsearch()
settings = {
    "mappings": {
        "news": {
            "properties": {
                "in_reply_to_screen_name": {
                    "type": "string",
                    "index" : "not_analyzed"
                },
                "events": {
                    "type": "nested",
                    "include_in_parent": True,
                    "properties": {
                        "text": {
                            "type": "string",
                            "index" : "not_analyzed"
                        },
                        "id": {
                            "type": "string",
                            "index" : "not_analyzed"
                        }
                    }
                },
                "urls": {
                    "properties": {
                        "url": {
                            "type": "string",
                            "index" : "not_analyzed"
                        },
                        "expanded_url": {
                            "type": "string",
                            "index" : "not_analyzed"
                        }
                    }
                },
                "hashtags": {
                    "type": "string",
                    "fields": {
                        "raw": {
                            "type": "string",
                            "index" : "not_analyzed"
                        }
                    }
                },
                "lang": {
                    "type": "string",
                    "index" : "not_analyzed"
                },
                "author_quoted": {
                    "type": "string",
                    "index" : "not_analyzed"
                },
                "author_retweeted": {
                    "type": "string",
                    "index" : "not_analyzed"
                },
                "user_screen_name": {
                    "type": "string",
                    "index" : "not_analyzed"
                },
                "user_mentions": {
                    "type": "string",
                    "index" : "not_analyzed"
                },
                "tags": {
                    "type": "string",
                    "index" : "not_analyzed"
                },
                "tweet_retweeted": {
                    "type": "string",
                    "index" : "not_analyzed"
                }
            }
        },
        "film": {
            "properties": {
                "in_reply_to_screen_name": {
                    "type": "string",
                    "index" : "not_analyzed"
                },
                "hashtags": {
                    "type": "string",
                    "fields": {
                        "raw": {
                            "type": "string",
                            "index" : "not_analyzed"
                        }
                    }
                },
                "lang": {
                    "type": "string",
                    "index" : "not_analyzed"
                },
                "author_quoted": {
                    "type": "string",
                    "index" : "not_analyzed"
                },
                "author_retweeted": {
                    "type": "string",
                    "index" : "not_analyzed"
                },
                "user_screen_name": {
                    "type": "string",
                    "index" : "not_analyzed"
                },
                "user_mentions": {
                    "type": "string",
                    "index" : "not_analyzed"
                },
                "films": {
                    "type": "string",
                    "index" : "not_analyzed"
                }
            }
        }
    }
}

def reindex():
    # create index
    if not es.indices.exists("tweets_index"):
        res_index = es.indices.create(index="tweets_index", ignore=400, body=settings)
        print("index created: ", res_index)

    res = helpers.reindex(es,"tests_index","tweets_index")
    print(res)

def getTweetsType(type):
    query = {
        "query" : {
            "constant_score" : {
                "filter" : {
                    "bool" : {
                        "must" : [
                            {"term": {"_type": type}},
                                    {"term": {"_index": "tweets_index"}},
                            {"exists" : { "field" : "events" }},
                            {"exists" : { "field" : "events.date" }}
                        ],
                        "must_not" : [
                            { "term": { "_id": "761549193395576834"}},
                            { "term": { "tags": "_filter"}},
                            { "term" : {"tags" : "_sample"}}
                        ]
                    }
                }
            }
        },
        "fields": ["events.id", "events.date", "events.text"],

        # "query": {
        #     "bool": {
        #         "must_not": [
        #             { "term": { "tags": "_filter"}}],
        #         "must_not": [{ "match": { "tags": "_filter"}}],
        #         "filter" :[
        #             {"term": {"_type": type}},
        #             {"term": {"_index": "tweets_events"}}
        #         ]
        #     }
        # },
    }

    get_tweets = helpers.scan(es, query = query)
    return get_tweets

def getTweetsID(id):
    query = {
        "query": {
            "match": {
                "events.id": id
            }
        }
    }
    get_tweets = helpers.scan(es, query = query)
    return get_tweets

def reloadBrexit():
    dirpath = "/home/bmazoyer/Documents/TwitterSea/Nice2/"
    folders = [folder for folder in os.listdir(dirpath) if os.path.isdir(dirpath + folder)]
    counter = 0

    for folder in folders:
        counter += 1
        print(folder)
        print (counter/len(folders))
    # folder = ""

        for file in [x for x in os.listdir(dirpath + folder) if os.path.isfile(dirpath + folder + "/" + x)]:
            stream = open(dirpath + folder + "/" + file, "r+", encoding='utf-8')
            file = stream.read()
            result = "[" + re.sub(r']\n\[',r'],\n[', file) + "]"
            text = json.loads(result)
            to_update = (
            {
                '_op_type': 'update',
                '_type':'news',
                '_index':'tweets_index',
                '_id': tweet[2]["id"],
                'script': "if (ctx._source.containsKey(\"tags\")) {ctx._source.tags = (ctx._source.tags + tag).unique()} else {ctx._source.tags = [tag]}",
                'params': {
                    'tag': "_Nice"
                },
                'upsert': {
                    'text': tweet[2]["text"],
                    'lang': tweet[2]["lang"],
                    'user_screen_name': tweet[2]["user"]["screen_name"],
                    'hashtags': " ".join(sorted([tweet[2]["entities"]["hashtags"][i]["text"] for i in range(len(tweet[2]["entities"]["hashtags"]))])) if tweet[2]["entities"]["hashtags"] != [] else None,
                    'urls' :  tweet[2]["entities"]["urls"],
                    'created_at' : datetime.strptime(
                    tweet[2]["created_at"],
                    "%a %b %d %H:%M:%S +0000 %Y").strftime("%Y-%m-%dT%H:%M:%S"
                    ),
                    'timestamp_ms' : datetime.fromtimestamp(int(tweet[2]["timestamp_ms"])/1000.0).strftime("%Y-%m-%dT%H:%M:%S.%f") if "timestamp_ms" in tweet[2] else None,
                    'collection_date' : tweet[0],
                    'favorite_count' : tweet[2]["favorite_count"],
                    'retweet_count' : tweet[2]["retweet_count"],
                    'user_mentions' :[user["screen_name"] for user in tweet[2]["entities"]["user_mentions"]],
                    'in_reply_to_screen_name' : tweet[2]["in_reply_to_screen_name"],
                    'author_quoted' : tweet[2]["quoted_status"]["user"]["screen_name"] if "quoted_status" in tweet[2] else None,
                    'is_retweet' : True if "retweeted_status" in tweet[2] else False,
                    'tweet_retweeted' :  tweet[2]["retweeted_status"]["id_str"] if "retweeted_status" in tweet[2].keys() else tweet[2]["id_str"],
                    'author_retweeted' : tweet[2]["retweeted_status"]["user"]["screen_name"] if "retweeted_status" in tweet[2] else None,
                    'tags' : ["_Nice"]
                }
            }
                  for tweet in text)

            try:
                tweets_not_created = []
                res = helpers.bulk(es,to_update,True)
                print(res)
            except helpers.BulkIndexError as error:
                res = error.errors
                for r in res:
                    tweets_not_created.append(r['update'])
                print("tweets not created", tweets_not_created)
            except IndexError as error:
                for tweet in text:
                    print(tweet)

def reloadNews():
    if not es.indices.exists("tweets_index"):
        res_index = es.indices.create(index="tweets_index", ignore=400, body=settings)
        print("index created: ", res_index)

    dirpath = "/home/ina/Documents/News/"
    folders = [folder for folder in os.listdir(dirpath) if os.path.isdir(dirpath + folder)]
    counter = 0

    for folder in folders:
        counter += 1
        print (counter/len(folders))
    # folder = ""

        for file in [x for x in os.listdir(dirpath + folder) if os.path.isfile(dirpath + folder + "/" + x)]:
            text = []
            stream = open(dirpath + folder + "/" + file, "r+", encoding='utf-8')
            for line in stream.readlines():
                try:
                    line = json.loads(line)
                    text.append(line)
                except json.decoder.JSONDecodeError as error:
                    print(error, file)
            to_update = (
            {
                '_op_type': 'update',
                '_type':'news',
                '_index':'tweets_index',
                '_id': tweet[2]["id"],
                'script': {
                    # "inline": "if (ctx._source.tags.contains(params.tag)) { ctx.op = \"none\" } else {ctx._source.tags.add(params.tag)}", #+
                    "inline" : "if (ctx._source.events.contains(params.event)) { ctx.op = \"none\" } else {ctx._source.events.add(params.event)}",
                    "lang": "painless",
                    "params" : {
                        "tag" : ("*"+file).strip(".json").strip("T_-1234567890").strip("*"),
                        "event" : {
                            "id": folder,
                            "date": datetime.strptime(
                                folder.strip(
                                "afp.com01234567890"
                                ).strip(
                                "-ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                                ),
                                "%Y%m%dT%H%M%S"
                                ).strftime("%Y-%m-%dT%H:%M:%S"
                            ),
                        }
                    }
                },
                'upsert': {
                    'text': tweet[2]["text"],
                    'lang': tweet[2]["lang"],
                    'user_screen_name': tweet[2]["user"]["screen_name"],
                    'hashtags': " ".join(sorted([tweet[2]["entities"]["hashtags"][i]["text"] for i in range(len(tweet[2]["entities"]["hashtags"]))])) if tweet[2]["entities"]["hashtags"] != [] else None,
                    'urls' :  tweet[2]["entities"]["urls"],
                    'created_at' : datetime.strptime(
                    tweet[2]["created_at"],
                    "%a %b %d %H:%M:%S +0000 %Y").strftime("%Y-%m-%dT%H:%M:%S"
                    ),
                    'timestamp_ms' : datetime.fromtimestamp(int(tweet[2]["timestamp_ms"])/1000.0).strftime("%Y-%m-%dT%H:%M:%S.%f") if "timestamp_ms" in tweet[2] else None,
                    'collection_date' : tweet[0],
                    'favorite_count' : tweet[2]["favorite_count"],
                    'retweet_count' : tweet[2]["retweet_count"],
                    'user_mentions' :[user["screen_name"] for user in tweet[2]["entities"]["user_mentions"]],
                    'in_reply_to_screen_name' : tweet[2]["in_reply_to_screen_name"],
                    'author_quoted' : tweet[2]["quoted_status"]["user"]["screen_name"] if "quoted_status" in tweet[2] else None,
                    'is_retweet' : True if "retweeted_status" in tweet[2] else False,
                    'tweet_retweeted' :  tweet[2]["retweeted_status"]["id_str"] if "retweeted_status" in tweet[2].keys() else tweet[2]["id_str"],
                    'author_retweeted' : tweet[2]["retweeted_status"]["user"]["screen_name"] if "retweeted_status" in tweet[2] else None,
                    'tags' : [("*"+file).strip(".json").strip("T_-1234567890").strip("*")],
                    "events" : [{
                        "id": folder,
                        "date": datetime.strptime(
                            folder.strip(
                            "afp.com01234567890"
                            ).strip(
                            "-ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                            ),
                            "%Y%m%dT%H%M%S"
                            ).strftime("%Y-%m-%dT%H:%M:%S"
                        )
                    }]
                }
            }
                  for tweet in text)

            try:
                res = helpers.bulk(es,to_update,True)
                print(res)
            except helpers.BulkIndexError as error:
                res = error.errors
                for r in res:
                    tweets_not_created.append(r['update'])
                print("tweets not created", tweets_not_created)
            except IndexError as error:
                for tweet in text:
                    print(tweet)




def reloadFilms():
    if not es.indices.exists("tests_index"):
      res_index = es.indices.create(index="tests_index", ignore=400, body=settings)
      print("index created: ", res_index)

    # reload all films related tweets with new hashtag format
    dirpath = "/home/bmazoyer/Documents/TwitterSea/Films/"
    folders = [folder for folder in os.listdir(dirpath) if os.path.isdir(dirpath + folder) and folder != "__pycache__"]
    counter = 0
    for folder in folders:
        print(folder)
        counter += 1
        for file in [file for file in os.listdir(dirpath + folder) if "trackfile" not in file]:
            stream = open(dirpath + folder + "/" + file, "r+", encoding='utf-8')
            file = stream.read()
            result = "[" + re.sub(r']\n\[',r'],\n[', file) + "]"
            text = json.loads(result)
            to_update = (
            {
                '_op_type': 'update',
                '_type':'film',
                '_index':'tests_index',
                '_id': tweet[2]["id"],
                'script': "if (ctx._source.containsKey(\"films\")) {ctx._source.films = (ctx._source.films + film).unique()} else {ctx._source.films = [films]}",
                'params': {
                    'film': folder
                },
                'upsert': {
                    'text': tweet[2]["text"],
                    'lang': tweet[2]["lang"],
                    'user_screen_name': tweet[2]["user"]["screen_name"],
                    'hashtags': " ".join(sorted([tweet[2]["entities"]["hashtags"][i]["text"] for i in range(len(tweet[2]["entities"]["hashtags"]))])) if tweet[2]["entities"]["hashtags"] != [] else None,
                    'urls' :  tweet[2]["entities"]["urls"],
                    'created_at' : datetime.strptime(
                    tweet[2]["created_at"],
                    "%a %b %d %H:%M:%S +0000 %Y").strftime("%Y-%m-%dT%H:%M:%S"
                    ),
                    'timestamp_ms' : datetime.fromtimestamp(int(tweet[2]["timestamp_ms"])/1000.0).strftime("%Y-%m-%dT%H:%M:%S.%f") if "timestamp_ms" in tweet[2] else None,
                    'collection_date' : tweet[0],
                    'favorite_count' : tweet[2]["favorite_count"],
                    'retweet_count' : tweet[2]["retweet_count"],
                    'user_mentions' :[user["screen_name"] for user in tweet[2]["entities"]["user_mentions"]],
                    'in_reply_to_screen_name' : tweet[2]["in_reply_to_screen_name"],
                    'author_quoted' : tweet[2]["quoted_status"]["user"]["screen_name"] if "quoted_status" in tweet[2] else None,
                    'is_retweet' : True if "retweeted_status" in tweet[2] else False,
                    'tweet_retweeted' :  tweet[2]["retweeted_status"]["id_str"] if "retweeted_status" in tweet[2].keys() else tweet[2]["id_str"],
                    'author_retweeted' : tweet[2]["retweeted_status"]["user"]["screen_name"] if "retweeted_status" in tweet[2] else None,
                    'films' : [folder]
                }
            }
                  for tweet in text)
            try:
                res = helpers.bulk(es,to_update,True)
                print(res)
            except helpers.BulkIndexError as error:
                res = error.errors
                for r in res:
                    tweets_not_created.append(r['update'])
                print("tweets not created", tweets_not_created)
            except IndexError as error:
                for tweet in text:
                    print(tweet)

        print (counter/len(folders))

def reloadEvents():
    tweets_not_created = []
    tweets = getTweetsType("news")
    to_update = (
    {
        '_op_type': 'update',
        '_type':'news',
        '_index':'tests_index',
        '_id': tweet["_id"],
        'doc': {
            'events': [{"id": tweet['fields']['events.id'][i], "text": tweet['fields']['events.text'][i], "date": tweet['fields']['events.date'][i]} for i in range(len(tweet['fields']['events.text']))]
        }
    }
          for tweet in tweets)

    try:
        res = helpers.bulk(es,to_update,True)
        print(res)
    except helpers.BulkIndexError as error:
        res = error.errors
        for r in res:
            tweets_not_created.append(r['update'])
        print("tweets not created", tweets_not_created)
    except IndexError as error:
        for tweet in text:
            print(tweet)
    except KeyError as error:
        print("KeyError")

def delete(id):
    tweets_not_deleted = []
    tweets = getTweetsID(id)
    to_delete = (
    {
        '_op_type': 'delete',
        '_type':'news',
        '_index':'tweets_index',
        '_id': tweet["_id"],
    }
          for tweet in tweets)

    try:
        res = helpers.bulk(es,to_delete,True)
        print(res)
    except helpers.BulkIndexError as error:
        res = error.errors
        for r in res:
            tweets_not_deleted.append(r['delete'])
        print("tweets not created", tweets_not_deleted)
    except IndexError as error:
        for tweet in text:
            print(tweet)
    except KeyError as error:
        print("KeyError")

if __name__ == "__main__":
    reloadNews()
