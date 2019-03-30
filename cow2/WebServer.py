import json

from bottle import Bottle, response, request, abort, run


def setup_routes(cow, host='localhost', port=8080):
    server = Bottle()

    @server.hook('after_request')
    def hook_allow_cross_origin():
        response.headers[
            'Access-Control-Allow-Origin'] = '*'
        response.headers[
            'Access-Control-Allow-Methods'] = 'GET, POST'
        response.headers[
            'Access-Control-Allow-Headers'] = 'Origin, Accept, Content-Type, X-Requested-With, X-CSRF-Token'

    @server.get('/')
    def get_keywords():
        response.content_type = 'application/json'
        keywords = cow.get_keywords()
        if len(keywords) == 0:
            abort(404, "No keywords available.")
        for keyword in keywords:
            if keyword['enabled'] == 0:
                keyword['enabled'] = False
            else:
                keyword['enabled'] = True

            keyword['tweet_count_range'] = {
                'min': keyword['min_tweet_count'],
                'max': keyword['max_tweet_count']
            }
            del keyword['min_tweet_count']
            del keyword['max_tweet_count']

        return json.dumps(keywords)

    @server.get('/<keyword_id:int>/cities/')
    def get_keyword_cities(keyword_id):
        response.content_type = 'application/json'
        cities = cow.get_all_cities_of_keyword(keyword_id)
        if len(cities) == 0:
            abort(404, "No cities available.")
        for city in cities:
            city['location'] = {
                'latitude': city['latitude'],
                'longitude': city['longitude']
            }
            del city['latitude']
            del city['longitude']
        return json.dumps(cities)

    @server.get('/<keyword_id:int>/cities/<city_id:int>/sentiment/')
    def get_keyword_cities(keyword_id, city_id):
        response.content_type = 'application/json'
        sentiments = cow.get_sentiments(keyword_id, city_id)
        if len(sentiments) == 0:
            abort(404, "No sentiments available.")
        for sentiment in sentiments:
            del sentiment['id']

        return json.dumps(sentiments)

    @server.post('/')
    def post_inquiry():
        post_data = json.loads(request.body.read())
        name = post_data['name']
        twitter_query = post_data['twitter_query']
        keyword_regex = post_data['keyword_regex']
        cities_list = post_data['cities_list']
        error = cow.add_keyword(name, twitter_query, keyword_regex, cities_list)
        if error is not None:
            abort(403, error)

        response.content_type = 'application/json'
        keywords = cow.get_keywords()
        return json.dumps(keywords)

    run(server, host=host, port=port)
