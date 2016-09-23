#!/usr/bin/env python
# -*- coding: utf-8 -*-
from os import listdir, path
from lxml import etree
from random import sample
from handleLimits import *
import string
from datetime import *
from elasticsearch import Elasticsearch, helpers
from unidecode import unidecode
from retrieveTweets import *
from sklearn import feature_extraction
import re
import redis
import csv
import collections

#stopwords
stopwords = ["a","alors","au","aucun","aussi","autre","aux","avant","avec","avoir","bon","c","ca","car","ce","cela","ces","ceux","chaque","ci","comme",
"comment","d","dans","de","des","dedans","dehors","depuis","devrait","doit","donc","du","elle","elles","en","encore","est",
"et","etaient","etions","ete","etre","eu","fait","faites","fois","font","hors","ici","il","ils","je","juste","l","la","le","les",
"leur","ma","maintenant","mais","mes","moins","mon","mot","meme","n","ne","ni","notre","nous","ont","ou","par","parce","pas","peut","peu","plupart",
"pour","pourquoi","quand","que","quel","quelle","quelles","quels","qui","s","sa","sans","ses","seulement","si","sien","son","sont","sous",
"soyez","sujet","sur","ta","tandis","tellement","tels","tes","ton","tous","tout","trop","tres","tu","un", "une","voient","vont","votre",
"vous","vu", "rt"]

def getEvents(start_date = None, end_date = None):
    es = Elasticsearch()
    query = {
        "size":0,
        "aggs": {
            "events": {
                "nested": {
                    "path": "events"
                },
                "aggs": {
                    "date_range": {
                        "filter": {
                            "bool": {
                                "must": {
                                    "range": {
                                        "events.date": {
                                            "gte": start_date,
                                            "lte": end_date
                                        }
                                    }
                                }
                            }
                        },
                        "aggs": {
                            "events": {
                                "terms": {
                                    "field": "events.id",
                                    "size": 5000
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    tweets = es.search(
        index="tests_index",
        doc_type = "news",
        body=query,
        filter_path="aggregations.events.date_range.events.buckets")
    return tweets["aggregations"]["events"]["date_range"]["events"]["buckets"]

def getFields(id, field):
    es = Elasticsearch()
    query = {
        "query": {
            "filtered": {
                "query": {
                    "match_all": {}
                },
                "filter": {
                    "nested": {
                        "path": "events",
                        "filter": {
                            "bool": {
                                "must": {
                                    "match": {
                                        "events.id": id
                                    }
                                }
                            }
                        }
                    }
                }
            }
        },
        "aggs" : {
            "fields" : {
                "terms" : {
                    "field" : field,
                    "size" : 5000
                }
            }
        }
    }
    tweets = es.search(index="tests_index", body=query)
    return tweets

# def getEvent(id):
#     es = Elasticsearch()
#     query = {
#     "query": {
#         "bool": {
#           "must": [{ "match": {
#             "events.id": {
#                 "query": id,
#                 "type": "phrase"
#             }
#           }}]
#         }
#       },
#     "fields": ["events.text", "events.id", "events.date"]
#     }
#     event = es.search(index="tests_index", doc_type="news", body=query)
#     if event['hits']['total'] == 0:
#         return(None)
#     else:
#         return event['hits']['hits'][0]

# def getTweetsevent(id):
#     es = Elasticsearch()
#     query = {
#     "query": {
#         "bool": {
#           "must": [{ "match": {
#             "events.id": {
#                 "query": id,
#                 "type": "phrase"
#             }
#           }}]
#         }
#       },
#     "fields": ["hashtags.text", "urls.expanded_url", "text"]
#     }
#
#     hashtags = helpers.scan(es, query = query)
#     return hashtags


def depecheAFP(date):
    #location of AFP dispatches
    dirpath = "/home/bmazoyer/Documents/AFP/" + date + "/"

    #dispatches not to analyze (weather, horoscope)
    exclude_dispatches = ["A la Une à", "À la une à", "A la une à", "Ils ont dit", "AGENDA HEBDO REGIONS", "A NOTER POUR AUJOURD HUI", "A NOTER POUR AUJOURD'HUI", "Le temps", "Le tour du monde des insolites", "Ce que disent les éditorialistes", "EN ATTENDANT DEMAIN", "APRES DEMAIN", "APRES-DEMAIN", "LUNDI", "MARDI", "MERCREDI", "JEUDI", "VENDREDI", "SAMEDI", "DIMANCHE"]

    #key-words that have no use for Twitter queries
    exclude = ['', 'francais', 'francaise', 'J', 'Son', 'France', 'Paris', 'parisien', 'AFP', 'parisienne']

    #type of search in Twitter API:
    method = "plain_search"

    #folder where the tweets will be saved
    filedir = "/home/bmazoyer/Documents/TwitterSea/News/"


    dispatches = {}
    Entities = collections.namedtuple('Entities', 'loc people orga')

    #name of the file containing named entities for AFP dispatches
    namesFile = "NamedEntities.csv"

    # collect only tweets emitted one the date of the dispatch (from 00:00:00)
    start_date = datetime.strptime(date, "%Y%m%d")
    tweet1 = retrieveHourlyTweet(None, round(time.mktime(start_date.timetuple())*1000))

    #files : list of AFP dispatches names
    files = list(listdir(dirpath))
    files.remove(namesFile)

    # Find titles of AFP dispatches
    for filename in files:
        tree = etree.parse(dirpath + filename)
        for x in tree.xpath("/NewsML/NewsItem/NewsComponent/DescriptiveMetadata/SubjectCode/Subject"):
            # exclude all dispatches concerning sport
            if x.get("FormalName") != "15000000":
                for x in tree.xpath("/NewsML/NewsItem/NewsComponent/NewsLines/HeadLine"):
                    title = x.text

                if title != None:
                    # exclude dispatches which name is in the exclude_dispatches list
                    check_list = [row not in title for row in exclude_dispatches]
                    if all(check_list):
                        # remove punctuation
                        for char in string.punctuation:
                            title=" ".join(title.replace(char," ").split())
                        # remove accents and special characters
                        title = unidecode(title).split()
                        #remove stopwords
                        for stopword in stopwords:
                            for word in title.copy():
                                if word.lower() == stopword:
                                    title.remove(word)
                        title = " ".join(title)
                        # test if the same name hasn't been already use for another dispatch
                        if title not in dispatches.values():
                            dispatches[filename] = title
    vocfile = "/home/bmazoyer/Documents/TwitterSea/vocabulary.json"
    stream = open(vocfile,"r+",encoding='utf-8')
    vocabulary = json.load(stream)
    stream.close()

    with open(dirpath + namesFile, 'r+') as csvfile:
        reader = csv.reader(csvfile, delimiter=';')
        for row in reader:
            #first element of each row in the file gives the dispatch ID
            filename = row[0].replace("file:" + dirpath, "")

            for n in range(1,4):
                for k in [k for k in " ".join([n for n in row[n].split(", ")]).split()]:
                    vocabulary[k.lower()] = ""


            if filename in dispatches:
                #find named entities in dispatch's title
                entities = Entities(
                loc = [" ".join(n.split()) for n in row[1].split(", ") if n not in exclude and n in dispatches[filename].split()],
                people = [k for k in " ".join([n for n in row[2].split(", ")]).split() if k not in exclude and k in dispatches[filename].split()],
                orga = [" ".join(n.split()) for n in row[3].split(", ") if n not in exclude and n in dispatches[filename].split()]
                )

                # remove duplicates
                unique_entities = set()
                queries = []
                for n in range(3):
                    for k in entities[n]:
                        unique_entities.add(k)

                # No queries with only one word
                if len(unique_entities) > 1:
                    for query in unique_entities:
                        # Avoid "first_name last_name": add query to list only if it contains a location or organisation
                        if query in entities[0] or query in entities[2]:
                            queries.append(" ".join(sorted(list(unique_entities))))
                            break
                queries.append(dispatches[filename])

                # each AFP dispatch is represented by an "event" in elasticsearch
                event = {}
                event["id"] = filename.strip(".xml")

                # date of AFP dispatch is in the name of the file
                event["date"] = datetime.strptime(
                filename.strip("ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789.xml"),
                "afp.com-%Y%m%dT%H%M%SZ-TX-PAR-").strftime("%Y-%m-%dT%H:%M:%S")
                event["text"] = dispatches[filename]

                # send query to Twitter API
                handle_limits_hourly(filedir, method, queries, event, tweet1["_id"])
                # hourly_search(filedir, method, queries, event, datetime.utcnow()-timedelta(days=1), hourly=False)

                # hashtags_tuples = {}
                # for tweet in getTweetsevent(event["id"]):
                #     if "fields" in tweet and "hashtags.text" in tweet["fields"]:
                #         hashtags_tuple = " ".join(sorted(tweet["fields"]["hashtags.text"]))
                #         if hashtags_tuple in hashtags_tuples:
                #             hashtags_tuples[hashtags_tuple] += 1
                #         else:
                #             hashtags_tuples[hashtags_tuple] = 1
                # print(hashtags_tuples)
    os.remove(vocfile)
    stream = open(vocfile,"a+",encoding='utf-8')
    stream.write(json.dumps(vocabulary))
    stream.close()

def max_match1(word, corpus):
    '''
    MaxMatch algorithm: segment a hashtag in words looking for the longest possible word first
    '''
    word = word.lower()
    length = len(word) # how far to iterate
    start_i = 0 # starting index
    matched_words = [] # word array to return

    # loop while we still have an index that is less than our entire word length
    while start_i < length:
        match_found = False
        # start at full length, end of word and decrement by 1 until index is at 0
        for i in range(length, 0, -1):
            # check if word from starting (initially 0 to end of word) is in corpus
            if (word[start_i:i] in corpus):
                # append from where the starting index to current index is
                matched_words.append(word[start_i:i])
                # found a match, not a single character
                match_found = True
                # increment to new starting position
                start_i = i
                break
        # We don't have a match, increment our starting point and add single character
        if not match_found:
            matched_words.append(word[start_i])
            start_i += 1
    return matched_words

def max_match2(word, corpus):
    '''
    MaxMatch algorithm: segment a hashtag in words looking for the longest possible word first
    '''
    word = word.lower()
    length = len(word) # how far to iterate
    start_i = length # starting index
    matched_words = [] # word array to return

    # loop while we still have an index that is less than our entire word length
    while start_i > 0:
        match_found = False
        # start at full length, end of word and decrement by 1 until index is at 0
        for i in range(length):
            # check if word from starting (initially 0 to end of word) is in corpus
            if (word[i:start_i] in corpus):
                # append from where the starting index to current index is
                matched_words.append(word[i:start_i])
                # found a match, not a single character
                match_found = True
                # increment to new starting position
                start_i = i
                break
        # We don't have a match, increment our starting point and add single character
        if not match_found:
            matched_words.append(word[start_i - 1])
            start_i -= 1
    return list(reversed(matched_words))

def date_to_str(time, *, days=0, seconds=0, minutes=0, hours=0):
    """
    Return time plus given timedelta in format "YYYY-MM-DDThh:mm:ss"
    Timedelta is given in days, hours, minutes, seconds.
    All key-words arguments are optional and default to zero.
    Arguments may be integers or floats and positive or negatives.
    """
    time = time + timedelta(days=days, seconds=seconds, minutes=minutes, hours=hours)
    return time.strftime("%Y-%m-%dT%H:%M:%S")

def analyzeEventsWords(event_date):
    # Take all words of all tweets for all events in a 24 hours period around event_date
    filename = "/home/bmazoyer/Documents/TwitterSea/vocabulary.json"
    stream = open(filename,"r+",encoding='utf-8')
    vocabulary = json.load(stream)
    stream.close()

    date = datetime.strptime(event_date, "%Y%m%d")
    # for time in range(24):
    # time = str(time) if time > 9 else "0"+ str(time)
    collection = []
    time = date + timedelta(hours = 12)
    events = [event['key'] for event in getEvents(date_to_str(time, days = -5), date_to_str(time, days = 5))]
    for event in events:
        hashtags = getFields(event, "hashtags.raw")["aggregations"]["fields"]["buckets"]
        if hashtags != [] and hashtags[0]['doc_count'] > 10:
            text = " ".join([n['doc_count']*(n['key']+" ") for n in hashtags ])
            collection.append(text)
        else:
            collection.append("")
    tf_idf = feature_extraction.text.TfidfVectorizer(stop_words = stopwords, max_df= 0.05, ngram_range = (1,3), use_idf = True)
    X =  tf_idf.fit_transform(collection)
    inversed_vocabulary = {v: k for k, v in tf_idf.vocabulary_.items()}

    hour = [event['key'] for event in getEvents(date_to_str(time, hours = -30), date_to_str(time, hours = 30))]
    for event in hour:
        print(event)
    for event in hour:
        print("tags: ", [tweet['key'].lower() for tweet in getFields(event, "tags")["aggregations"]["fields"]["buckets"]])
        i = events.index(event)
        frequent_words = set()
        for k in range(len(inversed_vocabulary)):
            if X[i,k] > 0.1:
                #alphabetical order to avoid duplicates in the "frequent_words" set
                word = " ".join(sorted(set(inversed_vocabulary[k].split())))
                frequent_words.add(word)

                    # print(inversed_vocabulary[k], X[i,k], tf_idf.idf_[k])
                    # print(events[i])
        frequent_words = list(frequent_words)
        frequent_words.sort(key = len)
        for word in frequent_words:
            if word in [tweet['key'].lower() for tweet in getFields(event, "tags")["aggregations"]["fields"]["buckets"]]:
                print(word, "already done")
            else:
                print(word)
            # if max_match2(word, vocabulary) == max_match1(word, vocabulary):
            #     print(" ".join(max_match2(word, vocabulary)), X[i, tf_idf.vocabulary_[word]])
            # else:
            #     print(word, X[i, tf_idf.vocabulary_[word]])
        print(" ")

if __name__ == "__main__":
    # depecheAFP("20160922")
    # print(getFields("afp.com-20160914T033833Z-TX-PAR-GVL39", "hashtags")["aggregations"]["hashtags"]["buckets"])
    # print(getEvents("2016-09-11T20:00:00", "2016-09-11T20:00:00"))
    analyzeEventsWords("20160922")
