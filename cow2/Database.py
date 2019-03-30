import sqlite3
from sqlite3.dbapi2 import Error

DB_CREATE_TABLE = [
    """CREATE TABLE IF NOT EXISTS config (
  id integer PRIMARY KEY,
  key text UNIQUE NOT NULL,
  value text NOT NULL
)""",
    """CREATE TABLE IF NOT EXISTS tweets (
  id integer PRIMARY KEY,
  body text NOT NULL,
  time text NOT NULL,
  keyword_city_id integer NOT NULL
)""",
    """CREATE TABLE IF NOT EXISTS sentiments (
  id integer PRIMARY KEY,
  score real,
  sentiment text,
  FOREIGN KEY (id) REFERENCES tweets (id)
)""",
    """CREATE TABLE IF NOT EXISTS keywords (
  id integer PRIMARY KEY,
  enabled integer NOT NULL,
  name text UNIQUE NOT NULL,
  twitter_query text NOT NULL,
  keyword_regex text NOT NULL,
  cities_list text NOT NULL,
  min_tweet_count integer,
  max_tweet_count integer,
  FOREIGN KEY (cities_list) REFERENCES cities_collection (name)
)""",
    """CREATE TABLE IF NOT EXISTS cities (
  id integer PRIMARY KEY,
  name text UNIQUE NOT NULL,
  latitude real NOT NULL,
  longitude real NOT NULL
)""",
    """CREATE TABLE IF NOT EXISTS cities_collection (
  id integer PRIMARY KEY,
  name text NOT NULL,
  city_id integer NOT NULL,
  FOREIGN KEY (id) REFERENCES cities (id)
)""",
    """CREATE TABLE IF NOT EXISTS keywords_cities (
  id integer NOT NULL PRIMARY KEY,
  keyword_id integer NOT NULL,
  city_id integer NOT NULL,
  cluster text,
  FOREIGN KEY (id) REFERENCES keywords (id),
  FOREIGN KEY (id) REFERENCES cities (id)
)"""
]

BG_SELECT_KEYWORDS = """
SELECT * FROM keywords
"""

BG_SELECT_KEYWORDS_BY_NAME = """
SELECT * FROM keywords WHERE keywords.name LIKE ?
"""

BG_SELECT_CITIES = """
SELECT * FROM cities WHERE cities.id IN (
  SELECT city_id from keywords_cities WHERE keywords_cities.keyword_id IS ?
)"""

BG_SELECT_SENTIMENT = """
SELECT *
FROM sentiments
WHERE sentiments.id IN (
  SELECT tweets.id
  FROM tweets
  WHERE tweets.keyword_city_id IN (
    SELECT keywords_cities.id
    FROM keywords_cities
    WHERE keywords_cities.keyword_id IS ?
      AND keywords_cities.city_id IS ?
  )
)"""

BG_INSERT_KEYWORDS = """
INSERT INTO keywords(enabled, name, twitter_query, keyword_regex, cities_list, min_tweet_count, max_tweet_count)
    VALUES (?, ?, ?, ?, ?, ?, ?)
"""

BG_INSERT_KEYWORDS_CITIES = """
INSERT INTO keywords_cities(keyword_id, city_id, cluster) VALUES (?, ?, ?) 
"""

BG_SELECT_CITIES_COLLECTION = """
SELECT * FROM cities WHERE cities.id IN (
    SELECT cities_collection.city_id FROM cities_collection WHERE cities_collection.name LIKE ?
)"""

BG_SELECT_COLLECTION_NAME = """
SELECT name FROM cities_collection WHERE cities_collection.id IS ?
"""

BG_SELECT_COLLECTION_ID = """
SELECT id FROM cities_collection WHERE cities_collection.name LIKE ?
"""

BG_SELECT_TWEETS = """
SELECT * FROM tweets WHERE tweets.keyword_city_id IN (
  SELECT keywords_cities.id FROM keywords_cities WHERE 
    keywords_cities.keyword_id IS ? AND
    keywords_cities.city_id IS ? 
)
"""

BG_INSERT_TWEET = """
INSERT OR IGNORE INTO tweets(id, body, time, keyword_city_id) VALUES (?, ?, ?, ?) 
"""

BG_INSERT_SENTIMENT = """
INSERT INTO sentiments(id, score, sentiment) VALUES (?, ?, ?) 
"""


def find_keyword_city_id(conn, keyword_id, city_id):
    c = conn.cursor()
    query = "SELECT id FROM keywords_cities WHERE keyword_id IS ? AND city_id IS ?"
    c.execute(query, (keyword_id, city_id))
    rows = c.fetchall()
    if len(rows) == 0:
        print("Tuple does not exist")

    if len(rows) > 1:
        print("Multiple entries found for keyword_city tuple")

    return rows[0][0]


def execute_query(conn, db_query, params=()):
    try:
        c = conn.cursor()
        c.execute(db_query, params)
        result = c.fetchall()
        return True
    except Error as e:
        print(e)
        return False


def create_connection(db_file):
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except Error as e:
        print(e)

    return None


def get_collection_name(conn, collection_id):
    c = conn.cursor()
    c.execute(BG_SELECT_COLLECTION_NAME, [collection_id])
    rows = c.fetchall()
    if len(rows) == 0 or len(rows) > 1:
        print("An issue occurred. Either none or multiple collections with the same name were found.")

    return rows[0][0]


def get_collection_id(conn, collection_name):
    c = conn.cursor()
    c.execute(BG_SELECT_COLLECTION_ID, [collection_name])
    rows = c.fetchall()
    if len(rows) == 0 or len(rows) > 1:
        print("An issue occurred. Either none or multiple collections with the same name were found.")
        print(rows)

    return rows[0][0]
