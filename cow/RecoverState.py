import json
import os

import cow


def process_city(keyword, city):
    tweets_path = cow.get_tweet_db_path(keyword, city)
    if not os.path.exists(tweets_path):
        print("State recovery detected missing tweets for %s - %s" % (keyword['name'], city['city']))
        print("This issue can be solved by letting the program run normally.")

    sentiment_path = cow.get_sentiment_db_path(keyword, city)
    if not os.path.exists(sentiment_path):
        print("State recovery detected missing sentiment for %s - %s" % (keyword['name'], city['city']))
        print("This issue can be solved by letting the program run normally.")

    sentiment_db_file = open(sentiment_path, mode='r', encoding='utf-8')
    sentiment_db = cow.prepare_sentiment_db_reader(sentiment_db_file)

    tweet_db_file = open(tweets_path, mode='r', encoding='utf-8')
    tweet_db = cow.prepare_tweet_db_reader(tweet_db_file)

    id_min = None
    id_max = None
    for tweet in tweet_db:
        id_current = tweet['id']
        if (id_min is None) or (id_current < id_min):
            id_min = id_current

        if (id_max is None) or (id_current > id_max):
            id_max = id_current

    # TODO save the state for live capabilities


def process_keyword(keyword):
    cities_path = "./config/" + keyword['cities_list'] + '.json'
    if not os.path.exists(cities_path):
        print("State recovery failed, could not find %s." % cities_path)
        exit(4)

    cities_file = open(cities_path, mode='r')
    cities = json.load(cities_file)
    cities_file.close()

    for city in cities:
        process_city(keyword, city)


def main():
    keywords_path = './config/keywords.json'
    if not os.path.exists(keywords_path):
        print("State recovery failed, could not find %s." % keywords_path)
        exit(4)

    keywords_file = open(keywords_path, mode='r')
    keywords = json.load(keywords_file)
    keywords_file.close()

    for keyword in keywords:
        process_keyword(keyword)
