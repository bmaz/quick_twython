from elasticsearch import Elasticsearch, helpers
from datetime import *
import time

def retrieveHourlyTweet(start_date = None, end_date = None):
    es = Elasticsearch()
    query = {
    "query": {
        "bool": {
            "must": [{ "match": { "_type": "news"}}],
            "filter": [{ "range": { "created_at": { "gte": start_date }}},
                       { "range": { "created_at": { "lte": end_date }}}]
        }
    },
    "size": 1,
    "sort": [
        {
            "created_at": {
                "order": "desc"
            }
        }
    ]
    }
    first_tweet = es.search(index="tweets_index", doc_type="news", body=query)
    if first_tweet['hits']['total'] == 0:
        return(None)
    else:
        return first_tweet['hits']['hits'][0]

def storeTweetsWithTag(tweets, query, event):
    tweets_not_created = []
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

    # create index
    if not es.indices.exists("tweets_index"):
        res_index = es.indices.create(index="tweets_index", ignore=400, body=settings)
        print("index created: ", res_index)

    to_update = (
    {
    '_op_type': 'update',
    '_type':'news',
    '_index':'tweets_index',
    '_id': tweet["id"],
    'script': "if (ctx._source.containsKey(\"tags\")) {ctx._source.tags = (ctx._source.tags + query).unique()} else {ctx._source.tags = [query]}; if (ctx._source.containsKey(\"events\")) {ctx._source.events = (ctx._source.events + event).unique()} else {ctx._source.events = [event]}",
    'params': {
        'query': query,
        'event': event
        },
    'upsert': {
        'text': tweet["text"],
        'lang': tweet["lang"],
        'user_screen_name': tweet["user"]["screen_name"],
        'hashtags': " ".join(sorted([tweet["entities"]["hashtags"][i]["text"] for i in range(len(tweet["entities"]["hashtags"]))])) if tweet["entities"]["hashtags"] != [] else None,
        'urls' :  tweet["entities"]["urls"],
        'created_at' : datetime.strptime(
        tweet["created_at"],
        "%a %b %d %H:%M:%S +0000 %Y").strftime("%Y-%m-%dT%H:%M:%S"
        ),
        'timestamp_ms' : datetime.fromtimestamp(int(tweet["timestamp_ms"])/1000.0).strftime("%Y-%m-%dT%H:%M:%S.%f") if "timestamp_ms" in tweet else None,
        'collection_date' : round(time.time()),
        'favorite_count' : tweet["favorite_count"],
        'retweet_count' : tweet["retweet_count"],
        'user_mentions' :[user["screen_name"] for user in tweet["entities"]["user_mentions"]],
        'in_reply_to_screen_name' : tweet["in_reply_to_screen_name"],
        'author_quoted' : tweet["quoted_status"]["user"]["screen_name"] if "quoted_status" in tweet.keys() else None,
        'is_retweet' : True if "retweeted_status" in tweet.keys() else False,
        'tweet_retweeted' :  tweet["retweeted_status"]["id_str"] if "retweeted_status" in tweet.keys() else tweet["id_str"],
        'author_retweeted' : tweet["retweeted_status"]["user"]["screen_name"] if "retweeted_status" in tweet.keys() else None,
        'tags' : [query],
        'events' : [event]
        }
    }
          for tweet in tweets if "id" in tweet)

    try:
        res = helpers.bulk(es,to_update,True)
        print(res)
    except helpers.BulkIndexError as error:
        res = error.errors
        for r in res:
            tweets_not_created.append(r['update'])
        print("tweets not created", tweets_not_created)
