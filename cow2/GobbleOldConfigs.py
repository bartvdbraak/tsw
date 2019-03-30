import csv
import json
import os

from cow2.Database import create_connection, find_keyword_city_id

INSERT_KEYWORDS = """
INSERT OR IGNORE INTO keywords(enabled, name, twitter_query, keyword_regex, cities_collection_name, min_tweet_count, max_tweet_count)
    VALUES (?, ?, ?, ?, ?, ?, ?)
"""

INSERT_CITIES = """
INSERT OR IGNORE INTO cities(name, latitude, longitude) VALUES (?, ?, ?)
"""

INSERT_KEYWORDS_CITIES = """
INSERT INTO keywords_cities(keyword_id, city_id, cluster) VALUES (?, ?, ?) 
"""

INSERT_CITIES_COLLECTION = """
INSERT INTO cities_collection(name, city_id) VALUES (?, ?) 
"""

INSERT_TWEET = """
INSERT OR IGNORE INTO tweets(id, body, time, keyword_city_id) VALUES (?, ?, ?, ?) 
"""

INSERT_SENTIMENT = """
INSERT OR IGNORE INTO sentiments(id, score, sentiment) VALUES (?, ?, ?) 
"""


def find_city_collection_id(conn, cities_list_name):
    c = conn.cursor()
    query = "SELECT id FROM cities_collection WHERE name LIKE (?)"
    c.execute(query, [cities_list_name])
    rows = c.fetchall()
    if len(rows) == 0:
        print("City does not exist", cities_list_name)

    if len(rows) > 1:
        print("Multiple entries found.")

    return rows[0][0]


def do_keywords():
    conn = create_connection("./db.sqlite")

    keywords_file = open('../config/keywords.json', mode='r')
    keywords = json.load(keywords_file)
    keywords_file.close()

    def keyword_to_tuple(keyword):
        city_collection = find_city_collection_id(conn, keyword['cities_list'])
        enabled = keyword['enabled']
        name = keyword['name']
        twitter_query = keyword['twitter_query']
        min_count = keyword['tweet_count_range']['min']
        max_count = keyword['tweet_count_range']['max']
        return enabled, name, twitter_query, keyword['keyword_regex'], city_collection, min_count, max_count

    proper_list = map(keyword_to_tuple, keywords)

    for entry in proper_list:
        c = conn.cursor()
        c.execute(INSERT_KEYWORDS, entry)
        print(c.lastrowid, entry)

    conn.commit()
    conn.close()


def do_cities(cities_path):
    cities_file = open(cities_path, mode='r')
    cities = json.load(cities_file)
    cities_file.close()

    def city_to_tuple(city):
        return city['city'], city['location']['latitude'], city['location']['longitude']

    proper_list = map(city_to_tuple, cities)

    conn = create_connection("./db.sqlite")

    for entry in proper_list:
        c = conn.cursor()
        c.execute(INSERT_CITIES, entry)
        print(c.lastrowid, entry)

    conn.commit()
    conn.close()


def find_keyword_id(conn, keyword_name):
    c = conn.cursor()
    query = "SELECT id FROM keywords WHERE name LIKE (?)"
    c.execute(query, [keyword_name])
    rows = c.fetchall()
    if len(rows) == 0:
        print("Keyword does not exist", keyword_name)

    if len(rows) > 1:
        print("Multiple entries found.")

    return rows[0][0]


def find_city_id(conn, city_name):
    c = conn.cursor()
    query = "SELECT id FROM cities WHERE name LIKE (?)"
    c.execute(query, [city_name])
    rows = c.fetchall()
    if len(rows) == 0:
        print("City does not exist", city_name)

    if len(rows) > 1:
        print("Multiple entries found.")

    return rows[0][0]


def do_keywords_cities():
    conn = create_connection("./db.sqlite")

    keywords_file = open('../config/keywords.json', mode='r')
    keywords = json.load(keywords_file)
    keywords_file.close()

    for keyword in keywords:
        if "." in keyword['cities_list']:
            print("Path manipulation detected in city list config.")
            exit(-1)

        cities_file = open("../config/" + keyword['cities_list'] + '.json', mode='r')
        cities = json.load(cities_file)
        cities_file.close()

        keyword_id = find_keyword_id(conn, keyword['name'])

        for city in cities:
            city_id = find_city_id(conn, city['city'])
            cluster = None
            if 'cluster' in city:
                cluster = city['cluster']
            c = conn.cursor()
            c.execute(INSERT_KEYWORDS_CITIES, (keyword_id, city_id, cluster))
            print(c.lastrowid)

    conn.commit()
    conn.close()


def do_tweets():
    conn = create_connection("./db.sqlite")

    keywords_file = open('../config/keywords.json', mode='r')
    keywords = json.load(keywords_file)
    keywords_file.close()

    for keyword in keywords:
        cities_file = open("../config/" + keyword['cities_list'] + '.json', mode='r')
        cities = json.load(cities_file)
        cities_file.close()

        keyword_id = find_keyword_id(conn, keyword['name'])

        for city in cities:
            city_id = find_city_id(conn, city['city'])

            kc_id = find_keyword_city_id(conn, keyword_id, city_id)

            keyword_name = keyword['name'].replace(" ", "").lower()
            city_name = city['city'].replace(" ", "").lower()
            file_path = '../tweets/%s/%s.csv' % (keyword_name, city_name)
            if not os.path.exists(file_path):
                print("No such file", file_path, "skipping")
                continue
            db_file = open(file_path, mode='r', encoding='utf-8')
            csv_db = csv.DictReader(db_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)

            for tweet in csv_db:
                c = conn.cursor()
                c.execute(INSERT_TWEET, (tweet['id'], tweet['text'], 'Undefined', kc_id))
                print(c.lastrowid)

    conn.commit()
    conn.close()


def do_sentiments():
    conn = create_connection("./db.sqlite")

    keywords_file = open('../config/keywords.json', mode='r')
    keywords = json.load(keywords_file)
    keywords_file.close()

    for keyword in keywords:
        cities_file = open("../config/" + keyword['cities_list'] + '.json', mode='r')
        cities = json.load(cities_file)
        cities_file.close()

        keyword_id = find_keyword_id(conn, keyword['name'])

        for city in cities:
            city_id = find_city_id(conn, city['city'])

            kc_id = find_keyword_city_id(conn, keyword_id, city_id)

            keyword_name = keyword['name'].replace(" ", "").lower()
            city_name = city['city'].replace(" ", "").lower()
            file_path = '../sentiment/%s/%s.csv' % (keyword_name, city_name)
            if not os.path.exists(file_path):
                print("No such file", file_path, "skipping")
                continue
            db_file = open(file_path, mode='r', encoding='utf-8')
            csv_db = csv.DictReader(db_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)

            for sentiment in csv_db:
                c = conn.cursor()
                sentiment_json = sentiment['sentiment'].replace('\'', '\"')
                valuation = json.loads(sentiment_json)
                c.execute(INSERT_SENTIMENT, (sentiment['id'], valuation['score'], valuation['label']))
                print(c.lastrowid)

    conn.commit()
    conn.close()


def do_city_lists(cities_path, name):
    conn = create_connection("./db.sqlite")

    cities_file = open(cities_path, mode='r')
    cities = json.load(cities_file)
    cities_file.close()

    for city in cities:
        city_id = find_city_id(conn, city['city'])
        c = conn.cursor()
        c.execute(INSERT_CITIES_COLLECTION, (name, city_id))

    conn.commit()
    conn.close()


do_keywords()
