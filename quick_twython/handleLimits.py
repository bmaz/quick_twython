from restApi import *
from config import *

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

if __name__ == "__main__":
    filedir = "/home/bmazoyer/Documents/TwitterSea/Tests/"
    results = []
    queries = queries = ["Nice", "#Nice06", "#NiceAttentat", "Promenade des Anglais", "Camion prom"]
    method = "plain_search"
    test = handle_limits(method, queries)
