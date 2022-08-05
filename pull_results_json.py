#! /usr/bin/env python

import requests
import subprocess

categories = (
    'Novel', 'Novella', 'Novelette', 'ShortStory',
    "RelatedWork", "GraphicStory", "DramaticLong",
    "DramaticShort", "EditorLong", "EditorShort",
    "ProArtist", "Semiprozine", "Fanzine", "Fancast",
    "FanWriter", "FanArtist", "Series", "Lodestar", "Astounding"
)

login = requests.get('https://local.hugo-nominations.tocotox.org/api/login?email=hugo-admin@example.com&key=key')
login_cookie_jar = login.cookies

for category in categories:
    results_json = requests.get(
        f'https://local.hugo-nominations.tocotox.org/api/hugo/admin/votes/{category}',
        cookies=login_cookie_jar
    )
    with open(f'results/{category}.json', 'w') as filehandle:
        filehandle.write(results_json.text)
    with open(f'results/{category}.json', 'r') as json_filehandle:
        with open(f'results/{category}.csv', 'w') as csv_filehandle:
            subprocess.run(
                ['python', 'results_from_json.py'],
                stdin=json_filehandle,
                stdout=csv_filehandle
            )
