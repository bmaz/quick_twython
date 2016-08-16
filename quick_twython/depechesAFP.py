#!/usr/bin/env python
# -*- coding: utf-8 -*-
from os import listdir, path
from lxml import etree
from random import sample
from handleLimits import *
import string
from datetime import *
from elasticsearch import Elasticsearch, helpers
import csv
import collections

def getEvent(id):
    es = Elasticsearch()
    query = {
    "query": {
        "bool": {
          "must": [{ "match": {
            "events.id": {
                "query": id,
                "type": "phrase"
            }
          }}]
        }
      },
    "fields": ["events.text", "events.id", "events.date"]
    }
    event = es.search(index="tweets_index", doc_type="news", body=query)
    if event['hits']['total'] == 0:
        return(None)
    else:
        return event['hits']['hits'][0]

def getTweetsevent(id):
    es = Elasticsearch()
    query = {
    "query": {
        "bool": {
          "must": [{ "match": {
            "events.id": {
                "query": id,
                "type": "phrase"
            }
          }}]
        }
      },
    "fields": ["hashtags.text", "urls.expanded_url"]
    }
    hashtags = es.search(index="tweets_index", doc_type="news", body=query)

    hashtags = helpers.scan(es, query = query)
    return hashtags


def depecheAFP(date):
    dirpath = "/home/bmazoyer/Documents/AFP/" + date + "/"
    exclude_dispatches = ["A la Une à", "Ils ont dit", "AGENDA HEBDO REGIONS", "A NOTER POUR AUJOURD HUI", "Le temps", "Le tour du monde des insolites", "Ce que disent les éditorialistes", "EN ATTENDANT DEMAIN", "APRES DEMAIN"]
    exclude = ['', 'francais', 'francaise', 'J', 'Son', 'France', 'Paris', 'français', 'parisien', 'AFP', 'française', 'parisienne']
    method = "plain_search"
    filedir = "/home/bmazoyer/Documents/TwitterSea/News/"
    dispatches = {}
    Entities = collections.namedtuple('Entities', 'loc people orga')
    namesFile = "NamedEntities.csv"

    files = list(listdir(dirpath))
    files.remove(namesFile)

    # Find titles of AFP dispatches and remove punctuation
    for filename in files:
        tree = etree.parse(dirpath + filename)
        for x in tree.xpath("/NewsML/NewsItem/NewsComponent/NewsLines/HeadLine"):
            title = x.text
            if title != None:
                for char in string.punctuation:
                    title=" ".join(title.replace(char," ").split())
                check_list = [row not in title for row in exclude_dispatches]
                if title not in dispatches.values() and all(check_list):
                    dispatches[filename] = title

    with open(dirpath + namesFile, 'r+') as csvfile:
        reader = csv.reader(csvfile, delimiter=';')
        for row in reader:
            filename = row[0].replace("file:" + dirpath, "")
            try:
                entities = Entities(
                loc = [" ".join(n.split()) for n in row[1].split(", ") if n not in exclude and n in dispatches[filename].split()],
                people = [k for k in " ".join([n for n in row[2].split(", ")]).split() if k not in exclude and k in dispatches[filename].split()],
                orga = [" ".join(n.split()) for n in row[3].split(", ") if n not in exclude and n in dispatches[filename].split()]
                )
            except Exception as error:
                continue

            queries = set()
            for n in range(3):
                for k in entities[n]:
                    queries.add(k)
            if len(queries) > 1:
                queries = [dispatches[filename], " ".join(list(queries))]
            else:
                queries = [dispatches[filename]]

            print(dispatches[filename])
            # print(queries)
            # print(entities[0], row[1].split(", "))
            # print(entities[1], row[2].split(", "))
            # print(entities[2], row[3].split(", "))

            event = {}
            event["id"] = filename.strip(".xml")
            event["date"] = datetime.strptime(
            filename.strip("ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789.xml"),
            "afp.com-%Y%m%dT%H%M%SZ-TX-PAR-").strftime("%Y-%m-%dT%H:%M:%S")
            event["text"] = dispatches[filename]
            # event["category"] = []
            handle_limits_hourly(filedir, method, queries, event)


if __name__ == "__main__":
    depecheAFP("20160812")
