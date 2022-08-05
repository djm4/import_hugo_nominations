#!/usr/bin/env python

import csv
import psycopg2 as dbm
from psycopg2.extras import Json
import configparser
from pprint import pprint


class HugoTranslator:
    category_map = {
        'Best Novel': 'Novel',
        'Best Novella': 'Novella',
        'Best Novelette': 'Novelette',
        'Best Short Story': 'ShortStory',
        'Best Related Work': 'RelatedWork',
        'Best Graphic Story or Comic': 'GraphicStory',
        'Best Dramatic Presentation, Long Form': 'DramaticLong',
        'Best Dramatic Presentation, Short Form': 'DramaticShort',
        'Best Editor, Long Form': 'EditorLong',
        'Best Editor, Short Form': 'EditorShort',
        'Best Professional Artist': 'ProArtist',
        'Best Semiprozine': 'Semiprozine',
        'Best Fanzine': 'Fanzine',
        'Best Fancast': 'Fancast',
        'Best Fan Writer': 'FanWriter',
        'Best Fan Artist': 'FanArtist',
        'Best Series': 'Series',
        'Astounding Award for Best New Writer, sponsored by Dell Magazines (not a Hugo)': 'Astounding',
        'Best Video Game': 'BestVideoGame',
        'Lodestar Award for Best Young Adult Book (not a Hugo)': 'Lodestar'
    }
    fields_map = {
        'Best Novel': ['title', 'author', 'publisher'],
        'Best Novella': ['title', 'author', 'publisher'],
        'Best Novelette': ['title', 'author', 'publisher'],
        'Best Short Story': ['title', 'author', 'publisher'],
        'Best Related Work': ['title', 'author', 'publisher'],
        'Best Graphic Story or Comic': ['title', 'author', 'publisher'],
        'Best Dramatic Presentation, Long Form': ['title', 'producer', 'p1'],
        'Best Dramatic Presentation, Short Form': ['title', 'series', 'producer'],
        'Best Editor, Long Form': ['editor', 'p1', 'p2'],
        'Best Editor, Short Form': ['editor', 'p1', 'p2'],
        'Best Professional Artist': ['author', 'example', 'p1'],
        'Best Semiprozine': ['title', 'p1', 'p2'],
        'Best Fanzine': ['title', 'p1', 'p2'],
        'Best Fancast': ['title', 'address', 'p1'],
        'Best Fan Writer': ['author', 'example', 'p1'],
        'Best Fan Artist': ['author', 'example', 'p1'],
        'Best Series': ['title', 'author', 'volume'],
        'Astounding Award for Best New Writer, sponsored by Dell Magazines (not a Hugo)': ['author', 'example', 'p1'],
        'Best Video Game': ['title', 'author', 'publisher'],
        'Lodestar Award for Best Young Adult Book (not a Hugo)': ['title', 'author', 'publisher']
    }

    def __init__(self):
        self.dbconn = None
        self.discon_dbconn = None
        self.config = None

    def read_config(self, filename='config.ini'):
        config = configparser.ConfigParser()
        config.read(filename)
        self.config = config

    def connect_db(self):
        dbconn = None
        params = self.config['kansa_db']
        try:
            dbconn = dbm.connect(
                database=params['database'],
                user=params['user'],
                password=params['password'],
                host=params['host'],
                port=params['port'])
        except dbm.OperationalError as err:
            print(f"Unable to connect to database {err}")
            exit(0)

        self.dbconn = dbconn

    def import_file(self):
        filename = self.config['file']['votes_filename']
        with open(filename, 'r') as fh:
            reader = csv.reader(fh)
            next(reader)   # Skip header row
            categorised_finalists_list = {}
            votes_list = {}
            for row in reader:
                current_ip, last_ip, current_ts, last_ts, contact_updated_ts, user_created_ts, \
                    contact_created_ts, ranks_created_ts, membership_id, email, \
                    preferred_first_name, preferred_last_name, title, first_name, last_name, \
                    category, finalist, position, \
                    *blank_fields = row
                normalised_category = self.category_map[category]
                if current_ts == '':
                    current_ts = '1970-01-01 00:00:00'
                votes_key = (membership_id, first_name, last_name, normalised_category)
                categorised_finalist = (normalised_category, finalist)
                with self.dbconn.cursor() as cursor:
                    if finalist == 'No Award':
                        finalist_id = -1
                    else:
                        query = """
                        SELECT id FROM hugo.finalists
                            WHERE competition='Hugos' AND category=%s AND title=%s
                        """
                        try:
                            cursor.execute(query, categorised_finalist)
                        except Exception as err:
                            print(f"Unable to run query {err}")
                            exit(0)
                        row = cursor.fetchone()
                        if row is None:
                            query = """
                            INSERT INTO hugo.finalists
                                (competition, category, sortindex, title, subtitle)
                                VALUES ('Hugos', %s, 1, %s, '') returning id
                            """
                            try:
                                cursor.execute(query, categorised_finalist)
                                row = cursor.fetchone()
                                categorised_finalists_list[categorised_finalist] = row[0]
                                print(f'Adding new entry, ID {categorised_finalists_list[categorised_finalist]}')
                            except Exception as err:
                                print(f"Unable to run query {err}")
                                exit(0)
                        else:
                            categorised_finalists_list[categorised_finalist] = row[0]
                            print("Entry already exists: skipping...", categorised_finalists_list[categorised_finalist])

                        finalist_id = categorised_finalists_list[(normalised_category, finalist)]
                if votes_key in votes_list:

                    votes_list[votes_key][int(position)] = finalist_id

                else:
                    votes_list[votes_key] = {
                        int(position): finalist_id
                    }

            self.dbconn.commit()
            cursor.close()
            pprint(votes_list)
            votes_rankings = {}

            for (vote_key, finalists) in votes_list.items():
                votes_rankings[vote_key] = []
                for rank in sorted(finalists.keys()):
                    votes_rankings[vote_key].append(finalists[rank])

            with self.dbconn.cursor() as cursor:
                for (vote_key, rankings) in votes_rankings.items():
                    membership_id, first_name, last_name, normalised_category = vote_key
                    row = None
                    query = """
                    SELECT id FROM hugo.votes
                        WHERE client_ip='127.0.0.1' AND client_ua='User Agent' AND competition='Hugos'
                            AND person_id=%s AND signature=%s AND category=%s
                    """
                    try:
                        cursor.execute(query, (
                            membership_id, f"{first_name} {last_name}", normalised_category
                        ))
                    except Exception as err:
                        print(f"Unable to run query {err}")
                        exit(0)
                    if cursor.fetchone() is None:
                        query = """
                        INSERT INTO hugo.votes
                            (client_ip, client_ua, person_id, signature, competition, category, votes)
                            VALUES ('127.0.0.1','User Agent', %s, %s, 'Hugos', %s, %s::integer[])
                        """
                        json_rankings = [Json(x) for x in rankings]
                        try:
                            cursor.execute(query, (
                                membership_id, f"{first_name} {last_name}", normalised_category, json_rankings
                            ))
                        except Exception as err:
                            print(f"Unable to run query {err}")
                            exit(0)
            self.dbconn.commit()
            cursor.close()


if __name__ == '__main__':
    translator = HugoTranslator()
    translator.read_config()
    translator.connect_db()
    translator.import_file()
