import datetime
import json
import os
import re

import twitter
from bottle import run, Bottle, request, response, abort
from watson_developer_cloud import NaturalLanguageUnderstandingV1
from watson_developer_cloud.natural_language_understanding_v1 import \
    Features, KeywordsOptions

import cow

service = NaturalLanguageUnderstandingV1(
    version='2018-03-16',
    url='https://gateway-lon.watsonplatform.net/natural-language-understanding/api',
    iam_apikey='4vh-qxAJcLf4u1pF9-PbCoLuOXJEcEyTMBv61G8DWP1a'
)

IBM_FEATURES = Features(keywords=KeywordsOptions(sentiment=True))

# api = twitter.Api(  # Bart
#     consumer_key='wFugeX7IuokpeY3ucuxcEsUVK',
#     consumer_secret='uaJE5kDJecUkmfe9BEBQfiykudNwiLEOMVecAR3SZwAjXHJZto',
#     access_token_key='114045402-85TSMdW61YFDYKDQDfZaBmefdPsbd8Y7SPj8rnTl',
#     access_token_secret='iudzD8iXIBEA8lol0RA7rwc3HA08ZhV2i4uaHXt3oiXYl',
#     sleep_on_rate_limit=False,
#     tweet_mode='extended'
# )

api = twitter.Api(  # Skip
    consumer_key='oJHJ2AqiVwwm6IYjIeK2Va7J9',
    consumer_secret='QvJJTGEINIOj3ZX5S9Dqi2HBB8mFijMzHhSWpFDHORStRGqvSD',
    access_token_key='3089963751-0NSpUJupoczS9C4bzgq1ZAgZFGs2J12wen3K6FL',
    access_token_secret='JCI0l8Jt4cmlNFfM6h4o4KUeasJNL8EkqTwOqW5NkSAiu',
    sleep_on_rate_limit=False,
    tweet_mode='extended'
)


def get_keyword_sentiment(keyword_regex, ibm_response):
    ibm_keywords = ibm_response['keywords']
    if len(ibm_keywords) > 0:
        for keyword in ibm_keywords:
            target = keyword['text']
            m = re.search(keyword_regex, target, re.IGNORECASE)
            if m is not None:
                return keyword['sentiment'], ibm_response['keywords']

    # If we do not find any matching keywords we will have to return nothing
    return None, ibm_response['keywords']


def regurgitate_city(keyword, tweet_db, sentiment_db):
    print("Getting sentiment over at most some 200 tweets")
    counter = 0
    for tweet in tweet_db:
        print("Tweet %d / ~200?" % counter)
        counter = counter + 1
        ibm_response = service.analyze(text=tweet['text'], features=IBM_FEATURES, language="en").get_result()
        sentiment, all_keywords = get_keyword_sentiment(keyword['keyword_regex'], ibm_response)
        if sentiment is None:
            concise = ["%s : %.3f" % (a['text'], a['sentiment']['score']) for a in all_keywords]
            print("Failed to get keyword sentiment keywords are: %s" % concise)
        else:
            sentiment_db.writerow({'id': tweet['id'], 'sentiment': sentiment})


def ibm_regurgitate(keyword, cities):
    print("Getting sentiment over %d cities" % len(cities))
    counter = 0
    for city in cities:
        print("City %d / %d" % (counter, len(cities)))
        counter = counter + 1

        sentiment_db_file_path = cow.get_sentiment_db_path(keyword, city)

        if os.path.exists(sentiment_db_file_path):
            print("Sentiment file for %s in %s already exists." % (keyword['name'], city['city']))
            continue

        if not os.path.exists(os.path.dirname(sentiment_db_file_path)):
            os.makedirs(os.path.dirname(sentiment_db_file_path))

        tweet_db_file_path = cow.get_tweet_db_path(keyword, city)
        if not os.path.exists(tweet_db_file_path):
            print("Tweets file for %s in %s does not exist." % (keyword['name'], city['city']))
            continue

        sentiment_db_file = open(sentiment_db_file_path, mode='w', encoding='utf-8')
        sentiment_db = cow.prepare_sentiment_db_writer(sentiment_db_file)

        tweet_db_file = open(tweet_db_file_path, mode='r', encoding='utf-8')
        tweet_db = cow.prepare_tweet_db_reader(tweet_db_file)

        regurgitate_city(keyword, tweet_db, sentiment_db)

        tweet_db_file.close()
        sentiment_db_file.close()


def slurp_city(keyword, city, csv_db):
    lat = city['location']['latitude']
    lon = city['location']['longitude']
    query_string = prepare_query_string(lat, lon, None, keyword['twitter_query'])
    results = []
    while len(results) < keyword['tweet_count_range']['max']:
        batch_results = api.GetSearch(raw_query=query_string)
        results.extend(batch_results)
        if len(batch_results) < 100:
            break
        query_string = prepare_query_string(lat, lon, batch_results[-1].id, keyword['twitter_query'])

    print("Got %d results for %s in %s" % (len(results), keyword['name'], city['city']))
    too_few_results = len(results) < keyword['tweet_count_range']['min']
    if too_few_results:
        print("Warning, unable to retrieve enough tweets from this city. qs=(%s)" % query_string)

    for status in results:
        csv_db.writerow({'id': status.id, 'text': status.full_text})

    return len(results)


def tweet_slurp(keyword, cities):
    count = 0
    for city in cities:
        file_path = cow.get_tweet_db_path(keyword, city)

        if os.path.exists(file_path):
            print("Tweets file for %s in %s already exists." % (keyword['name'], city['city']))
            continue

        if not os.path.exists(os.path.dirname(file_path)):
            os.makedirs(os.path.dirname(file_path))

        db_file = open(file_path, mode='w', encoding='utf-8')
        csv_db = cow.prepare_tweet_db_writer(db_file)
        count += slurp_city(keyword, city, csv_db)
        db_file.close()

    return count


def prepare_query_string(lat, lon, max_id, search_string):
    q = search_string + "%20AND%20" + "-filter" + "%3A" + "retweets"
    geocode = "%f" % lat + "%2C" + "%f" % lon + "%2C" + "25mi"
    lang = "en"
    result_type = "recent"
    count = "100"

    if max_id is None:
        return "q=%s&geocode=%s&lang=%s&result_type=%s&count=%s" % (
            q, geocode, lang, result_type, count
        )
    else:
        return "q=%s&geocode=%s&lang=%s&result_type=%s&count=%s&max_id=%s" % (
            q, geocode, lang, result_type, count, str(max_id)
        )


def keyword_from_id(keywords_json, keyword_id):
    for keyword in keywords_json:
        if keyword['id'] == keyword_id:
            if not keyword['enabled']:
                abort(404, "This keyword is not enabled.")
            if "." in keyword['cities_list']:
                abort(418, "This keyword has caused an issue.")
            return keyword
    abort(404, "This keyword is not available.")
    return None


def city_from_id(cities_json, city_id):
    for city in cities_json:
        if city['id'] == city_id:
            return city
    abort(404, "This city is not available.")


def read_cities(keyword):
    cities_file = open("./config/" + keyword['cities_list'] + '.json', mode='r')
    cities_json = json.load(cities_file)
    cities_file.close()
    return cities_json


def try_use_nlu(amount):
    nlu_path = "./config/nlu.json"
    if not os.path.exists(nlu_path):
        print("No NLU limit found, assuming unlimited use.")
        return True

    nlu_file = open(nlu_path, mode='r')
    nlu_json = json.load(nlu_file)
    nlu_file.close()

    now = datetime.datetime.now()
    nlu_file = open(nlu_path, mode='w')
    if nlu_json['year'] < now.year or nlu_json['month'] < now.month:
        nlu_json['year'] = now.year
        nlu_json['month'] = now.month
        nlu_json['used'] = 0
        json.dump(nlu_json, nlu_file)

    success = False
    if nlu_json['limit'] - nlu_json['used'] > amount:
        nlu_json['used'] = nlu_json['used'] + amount
        json.dump(nlu_json, nlu_file)
        success = True

    nlu_file.close()
    return success


def find_unused_id(keywords):
    return max(map(lambda kwd: kwd['id'], keywords)) + 1


def add_new_keyword(keywords, name, twitter_query, keyword_regex, cities_list):
    new_id = find_unused_id(keywords)
    new_keyword = {
        "id": new_id,
        "enabled": False,
        "name": name,
        "twitter_query": twitter_query,
        "keyword_regex": keyword_regex,
        "cities_list": cities_list,
        "tweet_count_range": {
            "min": 50,
            "max": 200
        }
    }
    keywords.append(new_keyword)

    cities_file = open("./config/" + new_keyword['cities_list'] + '.json', mode='r')
    cities_json = json.load(cities_file)
    cities_file.close()
    tweet_count = tweet_slurp(new_keyword, cities_json)

    has_capacity = try_use_nlu(tweet_count)
    new_keyword['enabled'] = has_capacity
    if has_capacity:
        ibm_regurgitate(new_keyword, cities_json)

    return new_keyword


def prepare_barf(keywords):
    server = Bottle()

    @server.hook('after_request')
    def cors_after_hook():
        response.headers['Access-Control-Allow-Origin'] \
            = '*'
        response.headers['Access-Control-Allow-Methods'] \
            = 'GET, POST'
        response.headers['Access-Control-Allow-Headers'] \
            = 'Origin, Accept, Content-Type, X-Requested-With, X-CSRF-Token'

    @server.get('/')
    def get_keywords():
        response.content_type = 'application/json'
        return json.dumps(keywords)
        # return json.dumps(list(filter(lambda kwd: (kwd['enabled']), keywords)))

    @server.get('/<keyword_id:int>/cities/')
    def get_keyword_cities(keyword_id):
        keyword = keyword_from_id(keywords, keyword_id)
        cities_json = read_cities(keyword)
        response.content_type = 'application/json'
        return json.dumps(cities_json)

    @server.get('/<keyword_id:int>/cities/<city_id:int>/sentiment/')
    def get_keyword_cities(keyword_id, city_id):
        keyword = keyword_from_id(keywords, keyword_id)
        cities_json = read_cities(keyword)
        city = city_from_id(cities_json, city_id)

        sentiment_db_file_path = cow.get_sentiment_db_path(keyword, city)
        if not os.path.exists(sentiment_db_file_path):
            abort(404, "Sentiment is not available %s" % sentiment_db_file_path)

        sentiment_db_file = open(sentiment_db_file_path, mode='r', encoding='utf-8')
        csv_dict_reader = cow.prepare_sentiment_db_reader(sentiment_db_file)
        print(sentiment_db_file_path)
        result = [json.loads(entry['sentiment'].replace('\'', '\"')) for entry in csv_dict_reader]
        sentiment_db_file.close()
        response.content_type = 'application/json'
        return json.dumps(result)

    @server.post('/')
    def post_inquiry():
        post_data = json.loads(request.body.read())
        name = post_data['name']
        if name in filter(lambda kwd: (kwd['name']), keywords):
            abort(403, "Keyword exists")

        twitter_query = post_data['twitter_query']
        if twitter_query in filter(lambda kwd: (kwd['twitter_query']), keywords):
            abort(403, "Twitter_query exists")

        keyword_regex = post_data['keyword_regex']
        if twitter_query in filter(lambda kwd: (kwd['twitter_query']), keywords):
            abort(403, "Keyword_regex exists")

        cities_list = post_data['cities_list']
        if cities_list not in ["cities_us_eu", "cities_global"]:
            abort(403, "City list does not exist.")

        new_keyword = add_new_keyword(keywords, name, twitter_query, keyword_regex, cities_list)
        return json.dumps(new_keyword)

    run(server, host='localhost', port=8080)


def main():
    # First we build a database
    keywords_file = open('./config/keywords.json', mode='r')
    keywords = json.load(keywords_file)
    keywords_file.close()

    for keyword in keywords:
        if not keyword['enabled']:
            continue

        if "." in keyword['cities_list']:
            print("Path manipulation detected in city list config.")
            exit(-1)

        cities_file = open("./config/" + keyword['cities_list'] + '.json', mode='r')
        cities_json = json.load(cities_file)
        cities_file.close()

        tweet_slurp(keyword, cities_json)

    # Now we wish to analyze the database
    for keyword in keywords:
        if not keyword['enabled']:
            continue

        cities_file = open("./config/" + keyword['cities_list'] + '.json', mode='r')
        cities_json = json.load(cities_file)
        cities_file.close()

        ibm_regurgitate(keyword, cities_json)

    prepare_barf(keywords)

    # for tweet in tweet_db:
    #     # response = service.analyze(text=tweet, features=features, language="en").get_result()
    #     sentiment = ibm_regurgitate(tweet, features, "flat.*earth")
    #     print(tweet + ":")
    #     print(json.dumps(sentiment, indent=2))
    #     print('============================================================')


main()
