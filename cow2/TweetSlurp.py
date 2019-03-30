import twitter

import cow2.Database as Database

api = twitter.Api(  # Skip
    consumer_key='<CONSUMER-KEY>',
    consumer_secret='<CONSUMER-SECRET>',
    access_token_key='<ACCESS-TOKEN-KEY>',
    access_token_secret='<ACCESS-TOKEN-SECRET>',
    sleep_on_rate_limit=True,
    tweet_mode='extended'
)


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


def slurp_city(conn, keyword, city):
    c = conn.cursor()
    c.execute(Database.BG_SELECT_TWEETS, (keyword['id'], city['id']))
    column_names = [col[0] for col in c.description]
    rows = c.fetchall()
    tweets = [dict(zip(column_names, row)) for row in rows]

    # For future live mode insert code for updating at start here

    if len(tweets) > 0:
        return

    lat = city['latitude']
    lon = city['longitude']

    query_string = prepare_query_string(lat, lon, None, keyword['twitter_query'])

    results = []
    while len(results) < keyword['max_tweet_count']:
        batch_results = api.GetSearch(raw_query=query_string)
        results.extend(batch_results)
        if len(batch_results) < 100:
            break
        query_string = prepare_query_string(lat, lon, batch_results[-1].id, keyword['twitter_query'])

    print("Got %d results for %s in %s" % (len(results), keyword['name'], city['name']))
    too_few_results = len(results) < keyword['min_tweet_count']
    if too_few_results:
        print("Warning, unable to retrieve enough tweets from this city. qs=(%s)" % query_string)

    for status in results:
        c = conn.cursor()
        keyword_city_id = Database.find_keyword_city_id(conn, keyword['id'], city['id'])
        c.execute(Database.BG_INSERT_TWEET, (status.id, status.full_text, status.created_at, keyword_city_id))

    conn.commit()


def slurp_keyword(conn, keyword):
    c = conn.cursor()
    c.execute(Database.BG_SELECT_CITIES, [keyword['id']])
    column_names = [col[0] for col in c.description]
    rows = c.fetchall()
    cities = [dict(zip(column_names, row)) for row in rows]
    for city in cities:
        slurp_city(conn, keyword, city)


def slurp(conn):
    c = conn.cursor()
    c.execute("SELECT * FROM keywords WHERE keywords.enabled")
    column_names = [col[0] for col in c.description]
    rows = c.fetchall()
    keywords = [dict(zip(column_names, row)) for row in rows]
    for keyword in keywords:
        slurp_keyword(conn, keyword)
