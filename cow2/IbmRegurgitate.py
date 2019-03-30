import re

from watson_developer_cloud import NaturalLanguageUnderstandingV1
from watson_developer_cloud.natural_language_understanding_v1 import KeywordsOptions, Features

import cow2.Database as Database

service = NaturalLanguageUnderstandingV1(
    version='2018-03-16',
    url='https://gateway-lon.watsonplatform.net/natural-language-understanding/api',
    iam_apikey='<API-KEY>'
)

IBM_FEATURES = Features(keywords=KeywordsOptions(sentiment=True))


def get_keyword_sentiment(regex, ibm_response):
    ibm_keywords = ibm_response['keywords']
    if len(ibm_keywords) > 0:
        for keyword in ibm_keywords:
            target = keyword['text']
            m = re.search(regex, target, re.IGNORECASE)
            if m is not None:
                return keyword['sentiment'], ibm_response['keywords']

    # If we do not find any matching keywords we will have to return nothing
    return None, ibm_response['keywords']


def regurgitate_city(conn, keyword, city):
    c = conn.cursor()
    c.execute(Database.BG_SELECT_SENTIMENT, (keyword['id'], city['id']))
    column_names = [col[0] for col in c.description]
    rows = c.fetchall()
    sentiments = [dict(zip(column_names, row)) for row in rows]

    # For future live mode insert code for updating at start here

    if len(sentiments) > 0:
        return

    c = conn.cursor()
    c.execute(Database.BG_SELECT_TWEETS, (keyword['id'], city['id']))
    column_names = [col[0] for col in c.description]
    rows = c.fetchall()
    tweets = [dict(zip(column_names, row)) for row in rows]

    if len(tweets) == 0:
        return
    else:
        print("Not enough sentiments, but there are some tweets for", keyword, city)

    for tweet in tweets:
        ibm_response = service.analyze(text=tweet['body'], features=IBM_FEATURES, language="en").get_result()
        sentiment, all_keywords = get_keyword_sentiment(keyword['keyword_regex'], ibm_response)

        if sentiment is None:
            concise = ["%s : %.3f" % (a['text'], a['sentiment']['score']) for a in all_keywords]
            print("Failed to get keyword sentiment keywords are: %s" % concise)

        else:
            c = conn.cursor()
            c.execute(Database.BG_INSERT_SENTIMENT, (tweet['id'], sentiment['score'], sentiment['label']))

    conn.commit()


def regurgitate_keyword(conn, keyword):
    c = conn.cursor()
    c.execute(Database.BG_SELECT_CITIES, [keyword['id']])
    column_names = [col[0] for col in c.description]
    rows = c.fetchall()
    cities = [dict(zip(column_names, row)) for row in rows]
    for city in cities:
        regurgitate_city(conn, keyword, city)


def regurgitate(conn):
    c = conn.cursor()
    c.execute("SELECT * FROM keywords WHERE keywords.enabled")
    column_names = [col[0] for col in c.description]
    rows = c.fetchall()
    keywords = [dict(zip(column_names, row)) for row in rows]
    for keyword in keywords:
        regurgitate_keyword(conn, keyword)
