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
from itertools import combinations
import re
import redis
import csv
import collections
import pickle
from scipy import sparse
import numpy as np

#stopwords
stopwords = ["00","000","01","02","03","04","05","06","07","08","09","a","alors","au","aucun","aussi","autre","aux","avant","avec","avoir","bon","c","ca","car","ce","cela","ces","ceux","chaque","ci","comme",
"comment","d","dans","de","des","dedans","dehors","depuis","devrait","doit","donc","du","elle","elles","en","encore","est",
"et","etaient","etions","ete","etre","eu","fait","faites","fois","font","hors","ici","il","ils","je","juste","l","la","le","les",
"leur","ma","maintenant","mais","mes","moins","mon","mot","meme","n","ne","ni","notre","nous","ont","ou","par","parce","pas","peut","peu","plupart",
"pour","pourquoi","quand","que","quel","quelle","quelles","quels","qui","s","sa","sans","ses","seulement","si","sien","son","sont","sous",
"soyez","sujet","sur","ta","tandis","tellement","tels","tes","ton","tous","tout","trop","tres","tu","un", "une","voient","vont","votre",
"vous","vu", "rt", 'francais', 'francaise', 'france', 'paris', 'parisien', 'afp', 'parisienne', 'Twitter', 'politique']
es = Elasticsearch()
def getEvents(start_date = None, end_date = None, min_doc_count = 25):
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
                                    "min_doc_count": min_doc_count,
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
        index="tweets_index",
        doc_type = "news",
        body=query,
        filter_path="aggregations.events.date_range.events.buckets")
    return tweets["aggregations"]["events"]["date_range"]["events"]["buckets"]

def countTweets(id):
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
        }
    }
    count = es.count(index="tweets_index", doc_type="news", body=query)['count']
    return count

def countRetweets(event_id, field, word):
    query = {
        "query": {
            "filtered": {
                "query": {
                    "bool": {
                      "must": [
                        { "match": { field : word } },
                        { "match": { "is_retweet": False } }
                      ]
                    }
                  },
                "filter": {
                    "nested": {
                        "path": "events",
                        "filter": {
                            "bool": {
                                "must": {
                                    "match": {
                                        "events.id": event_id
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    count = es.count(index="tweets_index", doc_type="news", body=query)['count']
    return count

def getTweetsEvent(id):
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
        }
    }
    tweets = helpers.scan(es, query = query)
    return tweets


def getFields(id, field, min_doc_count = 10):
    query = {
        "size": 0,
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
                    "min_doc_count": min_doc_count,
                    "size" : 5000
                }
            }
        }
    }
    tweets = es.search(index="tweets_index", body=query)["aggregations"]["fields"]["buckets"]
    return tweets

def getText(id):
    query = {
        "size": 0,
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
                    "field" : "tweet_retweeted",
                    "size" : 20000
                },
                "aggs": {
                    "first-hit": {
                        "top_hits": {
                            "_source": {
                                "include": [
                                    "text"
                                ]
                            },
                            "size" : 1

                        }
                    }
                }
            }
        }
    }
    tweets = es.search(
        index="tweets_index",
        body=query
    )['aggregations']['fields']['buckets']
    return tweets

def getEventsDetails(id):
    query = {
        "query": {
            "nested": {
                "path": "events",
                "query": {
                    "bool": {
                        "must": {
                            "match": {
                                "events.id": id
                            }
                        }
                    }
                }
            }
        },
        "fields": ["events.text", "events.date"],
        "size": 1
    }
    tweets = es.search(index="tweets_index", body=query)
    return tweets["hits"]["hits"][0]["fields"]

#type of search in Twitter API:
method = "plain_search"

#folder where the tweets will be saved
filedir = "/home/bmazoyer/Documents/TwitterSea/News/"

#location of AFP dispatches
AFPpath = "/home/bmazoyer/Documents/AFP/"

#key-words that have no use for Twitter queries
exclude = ['', 'francais', 'francaise', 'j', 'son', 'france', 'paris', 'parisien', 'afp', 'parisienne']

def depecheAFP(date):
    #location of AFP dispatches
    dirpath =  AFPpath + date + "/"

    #dispatches not to analyze (weather, horoscope, press review)
    exclude_dispatches = ["A la Une à", "À la une à", "A la une à", "Ils ont dit", "Ils l'ont dit", "AGENDA HEBDO REGIONS", "A NOTER POUR AUJOURD HUI", "A NOTER POUR AUJOURD'HUI", "Le temps", "Le tour du monde des insolites", "Ce que disent les éditorialistes", "EN ATTENDANT DEMAIN", "APRES DEMAIN", "APRES-DEMAIN", "LUNDI", "MARDI", "MERCREDI", "JEUDI", "VENDREDI", "SAMEDI", "DIMANCHE"]

    dispatches = {}
    Entities = collections.namedtuple('Entities', 'loc people orga')

    #name of the file containing named entities for AFP dispatches
    namesFile = "NamedEntities.csv"

    # collect only tweets emitted on the date of the dispatch
    start_date = datetime.strptime(date, "%Y%m%d")
    start_date = date_to_str(start_date, days=-1)
    tweet1 = retrieveHourlyTweet(None,start_date)

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
                    check_list = [phrase not in title for phrase in exclude_dispatches]
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
                loc = [" ".join(n.split()) for n in row[1].split(", ") if n.lower() not in exclude and n in dispatches[filename].split()],
                people = [k for k in " ".join([n for n in row[2].split(", ")]).split() if k.lower() not in exclude and k in dispatches[filename].split()],
                orga = [" ".join(n.split()) for n in row[3].split(", ") if n not in exclude and n.lower() in dispatches[filename].split()]
                )

                # remove duplicates
                unique_entities = set()
                queries = []
                for n in range(3):
                    for k in entities[n]:
                        unique_entities.add(k)

                # No queries with only one or two words
                if len(unique_entities) > 2:
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
                # print(method, queries, event, tweet1["_id"])
                handle_limits_hourly(filedir, method, queries, event)
                # hourly_search(filedir, method, queries, event, datetime.utcnow()-timedelta(hours=36), hourly=False)
#                 datetime.utcnow()-timedelta(days=1), hourly=False

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

def max_match(word):
    '''
    MaxMatch algorithm: segment a hashtag in words looking for the longest possible words
    '''
    filename = "/home/bmazoyer/Documents/TwitterSea/vocabulary.json"
    stream = open(filename,"r+",encoding='utf-8')
    corpus = json.load(stream)
    stream.close()

    word = word.lower()
    length = len(word) # how far to iterate
    start_i = 0 # starting index
    start_i_reverse = length
    matched_words = [] # word array to return
    matched_words_reverse = []

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
        # We don't have a match, break
        if not match_found:
            break


    while start_i_reverse > 0:
        match_found = False
        # start at full length, end of word and decrement by 1 until index is at 0
        for i in range(length):
            # check if word from starting (initially 0 to end of word) is in corpus
            if (word[i:start_i_reverse] in corpus):
                # append from where the starting index to current index is
                matched_words_reverse.append(word[i:start_i_reverse])
                # found a match, not a single character
                match_found = True
                # increment to new starting position
                start_i_reverse = i
                break
        # We don't have a match, break
        if not match_found:
            break

    matched_words_reverse = list(reversed(matched_words_reverse))
    if matched_words == matched_words_reverse:
        return " ".join(matched_words)
    else:
        return max(" ".join(min(matched_words, matched_words_reverse, key = len)), word, key = len)

def date_to_str(time, *, days=0, seconds=0, minutes=0, hours=0):
    """
    Return time plus given timedelta in format "YYYY-MM-DDThh:mm:ss"
    Timedelta is given in days, hours, minutes, seconds.
    All key-words arguments are optional and default to zero.
    Arguments may be integers or floats and positive or negatives.
    """
    time = time + timedelta(days=days, seconds=seconds, minutes=minutes, hours=hours)
    return time.strftime("%Y-%m-%dT%H:%M:%S")

def loadVoc():
    filename = "/home/bmazoyer/Documents/TwitterSea/vocabulary.json"
    stream = open(filename,"r+",encoding='utf-8')
    vocabulary = json.load(stream)
    stream.close()
    return vocabulary

def filterEvents(event_date, n_days=3):
    """Return all events in a n_days period around event_date"""
    date = datetime.strptime(event_date, "%Y%m%d") + timedelta(hours = 12)
    events = [event['key'] for event in getEvents(date_to_str(date, days = -n_days),
                                                  date_to_str(date, days = 1))]
    return date, events

def hashtagsMatrix(events):
    """shape a scipy.sparse count matrix out of hashtags refering to each event.
    Return the matrix and a vocabulary of form {"word": index_in_matrix}
    :param events: a list of ids. Ex: ['afp.com-20160922T065447Z-TX-PAR-HKZ50']"""

    hashtags = [{n['key']: n['doc_count'] for n in getFields(event, "hashtags.raw")} for event in events]
     # hashtags = [{"Sarkozy":2, "Buisson":3}, {"Hollande":4, "Sarkozy":1, "HollandeDehors":5}]

    voc = {}
    for doc in hashtags:
        for m in doc:
            if m not in voc:
                voc[m] = len(voc)
    #voc = {"Sarkozy":0, "Buisson":1, "Hollande":2, "HollandeDehors":3}

    X = sparse.lil_matrix((len(hashtags),len(voc)), dtype=int)
    for i, doc in enumerate(hashtags):
        X[i, [voc[m] for m in doc]] += np.array(list(doc.values()))


    return voc, X

def textMatrix(events):
    """shape a scipy.sparse count matrix out of words (1 to 3-grams) refering to each event.
    Return the matrix and a vocabulary of form {"word": index_in_matrix}
    :param events: a list of ids. Ex: ['afp.com-20160922T065447Z-TX-PAR-HKZ50']"""
    texts = [getText(event) for event in events]
    print("tweets loaded")
    events_size = [len(texts[i]) for i in range(len(texts))]
    collection = (
        re.sub(
            r'https?\S+|@\S+', '',tweet['first-hit']['hits']['hits'][0]['_source']['text']
        ) for n in texts for tweet in n
    )
    doc_counts = sparse.diags([tweet['doc_count'] for n in texts for tweet in n], dtype=int)
    vectorizer = feature_extraction.text.CountVectorizer(
        stop_words = stopwords,
        min_df = 2,
        ngram_range = (2,3),
        binary = True
    )
#     doc_counts*CountVectorizer = total frequency of words including retweets
    X = doc_counts.dot(vectorizer.fit_transform(collection))
    print("frequency matrix built")
#     build matrix with an event on each row.
    row = []
    for i in range(len(events)):
        row.extend([i for n in range(events_size[i])])
    col = range(X.shape[0])
    dat = [1 for n in range(X.shape[0])]
    S = sparse.csr_matrix((dat, (row, col)), shape=(len(events), X.shape[0]))

    return vectorizer.vocabulary_ , S*X

def tfIdfMatrix(words, events):
    """shape a tf_idf matrix out of tweets belonging to each event.
    Return the matrix and a vocabulary of form {"word": index_in_matrix}
    :param words: "text" or "hashtags" --> analyze all words or only hashtags
    :param events: a list of ids. Ex: ['afp.com-20160922T065447Z-TX-PAR-HKZ50']
    """

    ref_dict = {
        'hashtags': hashtagsMatrix,
        'text': textMatrix
    }

    voc, X = ref_dict[words](events)

    transformer = feature_extraction.text.TfidfTransformer()
    tf_idf = transformer.fit_transform(X)

    return voc, tf_idf

def analyzeEventsWords(event_date):
    # vocabulary = loadVoc()
    # print("vocabulary loaded")
    date, events = filterEvents(event_date)
    print("events filtered")
    inversed_vocabulary, X = tfIdfMatrix("text", events)
    print("tfidf matrix built")
    vocabulary = {v:k for k,v in inversed_vocabulary.items()}
    X.toarray().dump("/home/bmazoyer/Documents/TwitterSea/Matrix/tf_idf_text.mat")
    pickle.dump(vocabulary, open( "/home/bmazoyer/Documents/TwitterSea/Matrix/vocabulary_text.p", "wb"))
    
    hour = [event['key'] for event in getEvents(date_to_str(date, hours = -12), date_to_str(date, hours = 12))]
    for event in hour.copy():
        if event not in events:
            hour.remove(event)


    for event in hour:
        print(event)
        
        tags = [tweet['key'].lower() for tweet in getFields(event, "tags")]
        print("tags", tags)
        i = events.index(event)
        frequent_words = set()
        row = X.getrow(i)
#         find column index of maximum tf-idf value inside event's row
        while True:
            k = row.indices[row.data.argmax()] if row.nnz else 0
            if " ".join(sorted(set(vocabulary[k].split()))) in tags:
                row[0,k]=0
                row.eliminate_zeros()
                print(vocabulary[k], "in tags")
            else: 
                break
        
        frequent_words.add(" ".join(sorted(set(vocabulary[k].split()))))
        print(frequent_words)
        # for k in inversed_vocabulary:
        #     #alphabetical order to avoid duplicates in the "frequent_words" set
        #     word = " ".join(sorted(set(k.split())))
        #     if word not in tags:
        #         if X[i,inversed_vocabulary[k]] > 0.3:
        #             print(k, X[i,inversed_vocabulary[k]])
        #             frequent_words.add(word)
        #             # if X[i,k] > 0.3 and (len(word.split()) > 1 or len(max_match(word).split()) > 1) and countRetweets(event, "hashtags", word) > 1:
        #             if X[i,inversed_vocabulary[k]] > 0.3 and (len(word.split()) > 1):
        #                 print(k, X[i,inversed_vocabulary[k]])
        #                 print(word.split(), len(word.split()))
        #                 frequent_words.add(word)


#         complete_event = getEventsDetails(event)
#         complete_event = {
#             "id": event,
#             "text": complete_event["events.text"][0],
#             "date": complete_event["events.date"][0]
#         }
#         print(complete_event["text"])
#         date = datetime.strptime(complete_event["date"], "%Y-%m-%dT%H:%M:%S")
#         tweet1 = retrieveHourlyTweet(date_to_str(date, days=-1),date_to_str(date, days=1))
        
#         # if very_frequent_words != set():
#         #     print("very frequent words", very_frequent_words)
#         #     input("next ?")
#             # handle_limits_hourly(filedir, method, list(very_frequent_words), complete_event, tweet1["_id"])

#         if frequent_words != set():
#             depeche = ""
#             tree = etree.parse(AFPpath + event_date + "/" + event + ".xml")
#             node = tree.xpath("/NewsML/NewsItem/NewsComponent/ContentItem/DataContent/nitf/body/body.content")
#             for c in node:
#                 for x in c.getchildren():
#                     if x != None:
#                         depeche = depeche + str(x.text)
#             depeche = unidecode(depeche.lower())
#             query = []
#         else :
#             continue
#         max_words = 0
#         has_bigram = False
#         for word in frequent_words:
#             tags = [tweet['key'].lower() for tweet in getFields(event, "tags")]
#             if word not in tags:
#                 if all([subword in depeche for subword in word.split()]):
#                     query.append(word)
#                     max_words = max(max_words, len(word.split()))
#                     if len(word.split())==2:
#                         has_bigram = True
#                 elif all([max_match(subword) in depeche for subword in word.split()]):
#                     query.append(word)
#                     max_words = max(max_words, len(word.split()))
#                     if len(word.split())==2:
#                         has_bigram = True
#         if max_words == 1:
#             query = [" ".join(sorted(word)) for word in combinations(query, 2) if " ".join(sorted(word)) not in tags]
#         elif max_words == 2:
#             for word in query.copy():
#                 if len(word.split()) < max_words:
#                     query.remove(word)
#         else:
#             if has_bigram:
#                 query = [word for word in query if len(word.split()) == 2]
#             else:
#                 query = [word for word in query if len(word.split()) == max_words]
        if frequent_words != set():
            print("query", frequent_words)
            go = input("go ? y or n")
            if go == "y":
                handle_limits_hourly(filedir, method, query, complete_event, tweet1["_id"])


            # if max_match2(word, vocabulary) == max_match1(word, vocabulary):
            #     print(" ".join(max_match2(word, vocabulary)), X[i, tf_idf.vocabulary_[word]])
            # else:
            #     print(word, X[i, tf_idf.vocabulary_[word]])
        print(" ")

if __name__ == "__main__":
    # depecheAFP("20161115")
    # print(getEventsDetails("afp.com-20160914T033833Z-TX-PAR-GVL39"))
    # print(getEvents("2016-09-11T20:00:00", "2016-09-11T20:00:00"))
    # print(countTweets("afp.com-20160914T033833Z-TX-PAR-GVL39"))
    analyzeEventsWords("20161115")
