from elasticsearch import Elasticsearch, helpers
from datetime import *
import time

def retrieveHourlyTweet(start_date = None, end_date = None):
    es = Elasticsearch()
    query = {
    "query": {
        "bool": {
            "must": [{ "match": { "_type": "film"}}],
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
    first_tweet = es.search(index="tweets_index", doc_type="film", body=query)
    if first_tweet['hits']['total'] == 0:
        return(None)
    else:
        return first_tweet['hits']['hits'][0]

def storeTweetsWithTag(tweets, tag):
    tweets_not_created = []
    es = Elasticsearch()
    settings = {
        "mappings": {
            "urls": {
                "properties": {
                    "queries": {
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
    '_type':'brexit',
    '_index':'tweets_index',
    '_id': tweet["id"],
    'script': "if (ctx._source.containsKey(\"queries\")) {ctx._source.queries = (ctx._source.queries + query).unique()} else {ctx._source.queries = [query]}",
    'params': {
        'query': tag
        },
    'upsert': {
        'text': tweet["text"],
        'user_screen_name': tweet["user"]["screen_name"],
        'hashtags': tweet["entities"]["hashtags"],
        'created_at' : datetime.strptime(
        tweet["created_at"],
        "%a %b %d %H:%M:%S +0000 %Y").strftime("%Y-%m-%dT%H:%M:%S"
        ),
        'collection_date' : round(time.time()),
        'favorite_count' : tweet["favorite_count"],
        'retweet_count' : tweet["retweet_count"],
        'user_mentions' :[user["screen_name"] for user in tweet["entities"]["user_mentions"]],
        'in_reply_to_screen_name' : tweet["in_reply_to_screen_name"],
        'author_quoted' : tweet["quoted_status"]["user"]["screen_name"] if "quoted_status" in tweet.keys() else None,
        'is_retweet' : True if "retweeted_status" in tweet.keys() else False,
        'tweet_retweeted' :  tweet["retweeted_status"]["id"] if "retweeted_status" in tweet.keys() else None,
        'author_retweeted' : tweet["retweeted_status"]["user"]["screen_name"] if "retweeted_status" in tweet.keys() else None
        }
    }
          for tweet in tweets)
    try:
        res = helpers.bulk(es,to_update,True)
        print(res)
    except helpers.BulkIndexError as error:
        res = error.errors
        for r in res:
            tweets_not_created.append(r['update']['_id'])
        print("tweets not created", tweets_not_created)
