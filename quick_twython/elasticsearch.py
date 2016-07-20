from elasticsearch import Elasticsearch

def retrieveHourlyTweet(start_date = None, end_date = None):
    es = Elasticsearch()
    query = {
    "query": {
        "bool": {
            "must": [{ "match": { "_type": "brexit"}}],
            "filter": [{ "range": { "created_at": { "gte": start_date }}},
                       { "range": { "created_at": { "lte": end_date }}}]
        }
    },
    "size": 1
    }
    first_tweet = es.search(index="tweets_index", doc_type="brexit", body=query)
    if first_tweet['hits']['total'] == 0:
        return(None)
    else:
        return first_tweet['hits']['hits'][0]
