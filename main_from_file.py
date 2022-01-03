#!/usr/bin/env python

import csv
import psycopg2 as dbm
from psycopg2.extras import Json


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
        'Astounding Award for the Best New Writer, sponsored by Dell Magazines (not a Hugo)': 'NewWriter',
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
        'Astounding Award for the Best New Writer, sponsored by Dell Magazines (not a Hugo)': ['author', 'example', 'p1'],
        'Best Video Game': ['title', 'author', 'publisher'],
        'Lodestar Award for Best Young Adult Book (not a Hugo)': ['title', 'author', 'publisher']
    }

    def __init__(self):
        self.dbconn = None

    def connect_db(self):
        dbconn = None
        try:
            dbconn = dbm.connect(database="api", user="hugo", password="pwd", host="127.0.0.1", port="8432")
        except dbm.OperationalError as err:
            print(f"Unable to connect to database {err}")
            exit(0)

        self.dbconn = dbconn

    def import_file(self, filename):
        with open(filename, 'r') as fh:
            reader = csv.reader(fh)
            next(reader)   # Skip header row
            nominations_list = {}
            for row in reader:
                current_ip, last_ip, current_ts, last_ts, contact_updated_ts, user_created_ts, \
                    contact_created_ts, nominations_created_ts, membership_id, email, sign_in_count, \
                    preferred_first_name, preferred_last_name, first_name, last_name, \
                    category, nominations_field_1, nominations_field_2, nominations_field_3, \
                    *blank_fields = row
                normalised_category = self.category_map[category]
                category_fields = self.fields_map[category]
                nominations_key = (current_ts, current_ip, membership_id, first_name, last_name, normalised_category)
                if nominations_key in nominations_list:
                    nominations_list[nominations_key].append({
                        category_fields[0]: nominations_field_1,
                        category_fields[1]: nominations_field_2,
                        category_fields[2]: nominations_field_3,
                    })
                else:
                    nominations_list[nominations_key] = [{
                        category_fields[0]: nominations_field_1,
                        category_fields[1]: nominations_field_2,
                        category_fields[2]: nominations_field_3,
                    }]

            with self.dbconn.cursor() as cursor:
                for nominations_key in nominations_list:
                    print(nominations_key, nominations_list[nominations_key])
                    current_ts, current_ip, membership_id, first_name, last_name, normalised_category = nominations_key
                    row = None
                    query = """
                    SELECT id FROM hugo.nominations
                        WHERE time=%s AND client_ip=%s AND client_ua='User Agent' AND person_id=%s
                            AND signature=%s AND competition='Hugos' AND category=%s
                    """
                    try:
                        cursor.execute(query, (
                            current_ts, current_ip, membership_id, f"{first_name} {last_name}",
                            normalised_category
                        ))
                    except Exception as err:
                        print(f"Unable to run query {err}")
                        exit(0)
                    if cursor.fetchone() is not None:
                        print("Entry already exists: skipping...")
                        continue
                    query = """
                    INSERT INTO hugo.nominations
                        (time, client_ip, client_ua, person_id, signature, competition, category, nominations)
                        VALUES (%s, %s, 'User Agent', %s, %s, 'Hugos', %s, %s::jsonb[])
                    """
                    nominations = [Json(x) for x in nominations_list[nominations_key]]
                    try:
                        cursor.execute(query, (
                            current_ts, current_ip, membership_id, f"{first_name} {last_name}",
                            normalised_category, nominations
                        ))
                    except Exception as err:
                        print(f"Unable to run query {err}")
                        exit(0)
            self.dbconn.commit()
            cursor.close()


if __name__ == '__main__':
    translator = HugoTranslator()
    translator.connect_db()
    translator.import_file('nominations.csv')
