import csv


def get_tweet_db_path(keyword, city):
    keyword_name = keyword['name'].replace(" ", "").lower()
    city_name = city['city'].replace(" ", "").lower()
    file_path = './tweets/%s/%s.csv' % (keyword_name, city_name)
    return file_path


def prepare_tweet_db_reader(db_file):
    csv_db = csv.DictReader(db_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
    return csv_db


def prepare_tweet_db_writer(db_file):
    column_names = ['id', 'text']
    csv_db = csv.DictWriter(db_file, fieldnames=column_names, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
    csv_db.writeheader()
    return csv_db


def get_sentiment_db_path(keyword, city):
    keyword_name = keyword['name'].replace(" ", "").lower()
    city_name = city['city'].replace(" ", "").lower()
    file_path = './sentiment/%s/%s.csv' % (keyword_name, city_name)
    return file_path


def prepare_sentiment_db_reader(db_file):
    csv_db = csv.DictReader(db_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
    return csv_db


def prepare_sentiment_db_writer(db_file):
    column_names = ['id', 'sentiment']
    csv_db = csv.DictWriter(db_file, fieldnames=column_names, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
    csv_db.writeheader()
    return csv_db
