#! /usr/bin/env python

import csv
import json
import sys

vote_data = json.load(sys.stdin)

headings = ('Race for position', 'Finalist', 'Round 1', 'Round 2', 'Round 3', 'Round 4', 'Round 5', 'Round 6', 'Runoff')
finalists = []
vote_rows = []

for count in vote_data:
    for round in count['rounds']:
        for tally_item in round['tally']:
            if tally_item['finalist'] not in finalists:
                finalists.append(tally_item['finalist'])

for count in vote_data:
    place = count['place']
    vote_matrix = {}
    ranked_finalists = {}
    for round in count['rounds']:
        for tally_item in round['tally']:
            if tally_item['finalist'] in vote_matrix:
                vote_matrix[tally_item['finalist']].append(tally_item['votes'])
            else:
                vote_matrix[tally_item['finalist']] = [tally_item['votes']]
    for finalist in finalists:
        if finalist in vote_matrix:
            rounds_before_elimination = len(vote_matrix[finalist])
            if finalist == count['winner']:
                rounds_before_elimination += 1
            ranked_finalists[finalist] = rounds_before_elimination
    for finalist in [x[0] for x in sorted(ranked_finalists.items(), key=lambda finalist: finalist[1], reverse=True)]:
        if finalist in vote_matrix:
            vote_row = [place, finalist]
            vote_row.extend(vote_matrix[finalist])
            if len(vote_matrix[finalist]) < 6:
                vote_row.extend([''] * (6 - len(vote_matrix[finalist])))
            if finalist == 'No award':
                vote_row.append(count['runoff']['losses'])
            elif finalist == count['winner']:
                vote_row.append(count['runoff']['wins'])
            else:
                vote_row.append(0)
            vote_rows.append(vote_row)
    vote_rows.append(('', '', '', '', '', '', '', '', ''))

vote_writer = csv.writer(sys.stdout)
vote_writer.writerow(headings)

for vote_row in vote_rows:
    vote_writer.writerow(vote_row)

