import threading
from sqlite3 import Error

import cow2.Database
from cow2.IbmRegurgitate import regurgitate, regurgitate_keyword
from cow2.TweetSlurp import slurp, slurp_keyword
from cow2.WebServer import setup_routes

DEFAULT_DB_FILE = "./db.sqlite"


def add_keywords_cities(conn, keyword_id, cities_list):
    c = conn.cursor()
    c.execute(cow2.Database.BG_SELECT_CITIES_COLLECTION, [cities_list])
    column_names = [col[0] for col in c.description]
    rows = c.fetchall()
    cities = [dict(zip(column_names, row)) for row in rows]
    for city in cities:
        c2 = conn.cursor()
        c2.execute(cow2.Database.BG_INSERT_KEYWORDS_CITIES, (keyword_id, city['id'], None))
    conn.commit()


class Cow:
    def __init__(self, db_file):
        self.db_file = db_file
        self.conn = cow2.Database.create_connection(db_file)

    def start_web_server(self):
        setup_routes(self)

    def get_keywords(self):
        c = self.conn.cursor()
        c.execute(cow2.Database.BG_SELECT_KEYWORDS)
        column_names = [col[0] for col in c.description]
        rows = c.fetchall()
        return [dict(zip(column_names, row)) for row in rows]

    def get_all_cities_of_keyword(self, keyword_id):
        c = self.conn.cursor()
        c.execute(cow2.Database.BG_SELECT_CITIES, [keyword_id])
        column_names = [col[0] for col in c.description]
        rows = c.fetchall()
        return [dict(zip(column_names, row)) for row in rows]

    def get_sentiments(self, keyword_id, city_id):
        c = self.conn.cursor()
        c.execute(cow2.Database.BG_SELECT_SENTIMENT, [keyword_id, city_id])
        column_names = [col[0] for col in c.description]
        rows = c.fetchall()
        return [dict(zip(column_names, row)) for row in rows]

    def add_keyword(self, name, twitter_query, keyword_regex, cities_list):
        conn = cow2.Database.create_connection(self.db_file)
        c = conn.cursor()
        try:
            c.execute(cow2.Database.BG_INSERT_KEYWORDS,
                      (True, name, twitter_query, keyword_regex, cities_list, 50, 200))
            add_keywords_cities(conn, c.lastrowid, cities_list)
        except Error as e:
            print("An Error happened", e)
            return None

        c = conn.cursor()
        c.execute(cow2.Database.BG_SELECT_KEYWORDS_BY_NAME, [name])
        column_names = [col[0] for col in c.description]
        rows = c.fetchall()
        keywords = [dict(zip(column_names, row)) for row in rows]
        for keyword in keywords:
            self.slurp_regurgitate_keyword(keyword)

        return self.get_keywords()

    def halt(self):
        self.conn.close()

    def wait(self):
        pass

    def slurp_regurgitate(self):
        def do_both():
            conn = cow2.Database.create_connection(self.db_file)
            slurp(conn)
            regurgitate(conn)

        threading.Thread(target=do_both).start()

    def slurp_regurgitate_keyword(self, keyword):
        def do_both():
            conn = cow2.Database.create_connection(self.db_file)
            slurp_keyword(conn, keyword)
            regurgitate_keyword(conn, keyword)

        threading.Thread(target=do_both).start()


def recover_system(db_file):
    # Assert we can connect to the database
    conn = cow2.Database.create_connection(db_file)
    if conn is None:
        print("Unable to establish database connection.")
        exit(-1)

    for db_query in cow2.Database.DB_CREATE_TABLE:
        if not cow2.Database.execute_query(conn, db_query):
            exit(-1)

    conn.close()


def start(db_file=DEFAULT_DB_FILE):
    print("Cow v2 starting.")
    recover_system(db_file)
    cow = Cow(db_file)
    cow.slurp_regurgitate()
    print("Cow v2 started.")

    cow.start_web_server()
    print("Cow v2 stopped.")


start()
