import csv

import requests
from bs4 import BeautifulSoup
import re
from datetime import datetime
from tabulate import tabulate

def get_by(club = '-1', league = '-1', block = '416'):
    data = {
        'ctl00$cphMiddle$AjaxScriptManager1': 'ctl00$cphMiddle$AjaxScriptManager1|ctl00$cphMiddle$ddlClub',
        '__EVENTTARGET': 'ctl00$cphMiddle$ddlClub',
        '__EVENTARGUMENT': '',
        '__LASTFOCUS': '',
        '__VIEWSTATE': '/wEPDwUKMTczMDMwNTI3NA9kFgJmD2QWBAJTD2QWBgIBDw8WAh4HVmlzaWJsZWhkZAIFDw8WAh8AaGRkAgcPDxYCHwBoZGQCVQ9kFgICAQ9kFgICBw9kFgJmD2QWDgIBDw8WAh4EVGV4dAUbV2Vkc3RyaWpkZW4gc2VuaW9yZW4gLSB2ZWxkZGQCAw8QDxYGHg1EYXRhVGV4dEZpZWxkBQROYW1lHg5EYXRhVmFsdWVGaWVsZAUCSWQeC18hRGF0YUJvdW5kZ2QQFRYcVjAxOiAxOC8wOC8yMDE2IC0gMjEvMDgvMjAxNhxWMDI6IDI1LzA4LzIwMTYgLSAyOC8wOC8yMDE2G1YwMzogMzEvMDgvMjAxNiAtIDQvMDkvMjAxNhtWMDQ6IDcvMDkvMjAxNiAtIDExLzA5LzIwMTYcVjA1OiAxNC8wOS8yMDE2IC0gMTgvMDkvMjAxNhxWMDY6IDIxLzA5LzIwMTYgLSAyNS8wOS8yMDE2G1YwNzogMjgvMDkvMjAxNiAtIDIvMTAvMjAxNhpWMDg6IDUvMTAvMjAxNiAtIDkvMTAvMjAxNhxWMDk6IDEyLzEwLzIwMTYgLSAxNi8xMC8yMDE2HFYxMDogMTkvMTAvMjAxNiAtIDIzLzEwLzIwMTYcVjExOiAyNi8xMC8yMDE2IC0gMzAvMTAvMjAxNhpWMTI6IDIvMTEvMjAxNiAtIDYvMTEvMjAxNhxWMTQ6IDE1LzAzLzIwMTcgLSAxOS8wMy8yMDE3HFYxNTogMjIvMDMvMjAxNyAtIDI2LzAzLzIwMTcbVjE2OiAyOS8wMy8yMDE3IC0gMi8wNC8yMDE3GlYxNzogNS8wNC8yMDE3IC0gOS8wNC8yMDE3HFYxODogMTkvMDQvMjAxNyAtIDIzLzA0LzIwMTccVjE5OiAyNi8wNC8yMDE3IC0gMzAvMDQvMjAxNxpWMjA6IDMvMDUvMjAxNyAtIDcvMDUvMjAxNxxWMjE6IDEwLzA1LzIwMTcgLSAxNC8wNS8yMDE3HFZGMTogMTkvMDUvMjAxNyAtIDIxLzA1LzIwMTccVkYyOiAyNi8wNS8yMDE3IC0gMjgvMDUvMjAxNxUWAzQxNgM0MTcDNDE4AzQxOQM0MjADNDIxAzQyMgM0MjMDNDI0AzQyNQM0MjYDNDI3AzQyOAM0MjkDNDMwAzQzMQM0MzIDNDMzAzQzNAM0MzUDNDM2AzQzNxQrAxZnZ2dnZ2dnZ2dnZ2dnZ2dnZ2dnZ2dnFgFmZAIFDxAPFgYfAgUETmFtZR8DBQJJZB8EZ2QQFTQACUFHTyBBYWxzdAhBS0MvTHVtYQZBcHBlbHMEQVNLQwRBU09DBEFUQlMMQmV2ZXJlbi1XYWFzD0JvZWNob3V0LVZyZW1kZQtCb2Vja2VuYmVyZw1Cb3JnZXJob3V0L0dXBkJvcm5lbQlCb3V0ZXJzZW0FQ2F0YmEGRGV1cm5lBkVrZXJzZQhGbG9yaWFudAVHYW5kYQZHZWVsc2URSG9ib2tlbi9NZXJjdXJpdXMISG9ldmVuZW4ESG92ZQRKb2tpCEthcGVsbGVuCEtDIExpbWVzBEtDQkoES0NPVgRLd2lrBkxldXZlbgVMdWJrbwdNZWV1d2VuB01pbmVydmEKTmVlcmxhbmRpYQ1PLiBBbmRlcmxlY2h0BVB1dHNlBVJpamtvB1JpdmllcmEDUktDB1NjYWxkaXMGU2lrb3BpEFNpbnQtR2lsbGlzLVdhYXMMU3BhcnRhIFJhbnN0CVNwYXJ0YWN1cwhUZWNobmljbwVUZW1zZQ9UaGUgQmx1ZSBHaG9zdHMLVGhlIFZpa2luZ3MFVmVyZGUGVm9iYWtvClZvb3J3YWFydHMMVm9zIFJlaW5hZXJ0BVZvc2tvFTQCLTECNDYBNQIyOQI0MQI2MQIxOAI2OAI0NwIxMwIxNwIzMwI1NwIyMgIyMAIzMgIzNQIxMgI1MwIzOQIzNgI2MAIzNwIxOQI0NAI0OQI0NQIyMwI0OAI1NgIxMQE2AjE0AjIxAjM0AjUyAjEwAjU0ATQCMjgCNjQCMjYCMjUBMwI0MgI2MwI3MAE5ATEBNwEyAjUxFCsDNGdnZ2dnZ2dnZ2dnZ2dnZ2dnZ2dnZ2dnZ2dnZ2dnZ2dnZ2dnZ2dnZ2dnZ2dnZ2dnZ2dnZ2cWAWZkAg0PEA8WBh8CBQROYW1lHwMFAklkHwRnZBAVDwAJVE9QS0xBU1NFC1BST01PS0xBU1NFDUhPT0ZES0xBU1NFIDENSE9PRkRLTEFTU0UgMhFPVkVSR0FOR1NLTEFTU0UgQRFPVkVSR0FOR1NLTEFTU0UgQg1UT1BLTEFTU0UgcmVzD1BST01PS0xBU1NFIHJlcxFIT09GREtMQVNTRSAxIHJlcxFIT09GREtMQVNTRSAyIHJlcxVPVkVSR0FOR1NLTEFTU0UgQSByZXMTMXN0ZSBnZXcgVE9QS0xBU1NFIBQxc3RlIGdldyBQUk9NT0tMQVNTRRYxc3RlIGdldyBIT09GREtMQVNTRSAxFQ8CLTEEMTIwMAQxMjAxBDEyMDYEMTIwNwQxMjIyBDEyMjMEMTIwMgQxMjAzBDEyMTAEMTIxMQQxMjEyBDEyMDQEMTIwNQQxMjEzFCsDD2dnZ2dnZ2dnZ2dnZ2dnZxYBZmQCEw8PFgQeC05hdmlnYXRlVXJsBRsvS2xhc3NlbWVudGVuL3NlbmlvcmVuL3ZlbGQeB0VuYWJsZWRnZGQCFQ8WAh4LXyFJdGVtQ291bnQCOGQCFw8PFgYfBWUeBlRhcmdldAUGX2JsYW5rHwFlZGRkjcnAGIGfaNeZftlBLwp3L5+rnkA=',
        '__VIEWSTATEGENERATOR': 'BFB15B05',
        '__EVENTVALIDATION': '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',
        'ctl00$cphMiddle$ddlBlock': block,
        'ctl00$cphMiddle$ddlClub': club,
        'ctl00$cphMiddle$ddlLeague': league,
        '__ASYNCPOST': 'true'
    }
    headers = {
        'Accept-Language': 'nl-NL,nl;q=0.8,en-US;q=0.6,en;q=0.4,fr;q=0.2',
        'Origin': 'http://korfbal.be',
        'Referer': 'http://korfbal.be/Wedstrijden/senioren/veld',
        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.86 Safari/537.36'
    }
    req_url = 'http://korfbal.be/Wedstrijden/senioren/veld'
    r = requests.post(req_url, data=data, headers=headers)
    return r.text


def dict_from_table(soup, isclub=False):
    headers = ['nummer', None, 'datum','tijd','thuisploeg','bezoekers',
               'aantal_thuis', 'aantal_bezoekers', 'scheidsrechter', 'jury', 'locatie']
    if isclub:
        headers.insert(2,'reeks')
    results = [result for result in soup.find_all('td', class_=re.compile(r'rpt.*Item'))]
    answer = []
    cur = dict()
    n_headers = len(headers)
    colspan = 0
    for i, t in enumerate(results):
        if t.get('colspan'):
            colspan += int(t.get('colspan'))-1
            continue
        header_name = headers[(i + colspan)%n_headers]
        if header_name:
            cur[header_name] = t.text
        if (i+colspan)%n_headers == n_headers-1:
            answer.append(cur)
            cur = dict()

    return answer


def fix_format(d, reeks=''):
    def fix_row(row):
        if row.get('aantal_thuis', None):
            row['aantal_thuis'] = (row['aantal_thuis'][:-1])
        if row.get('aantal_bezoekers', None):
            row['aantal_bezoekers'] = (row['aantal_bezoekers'])
        row['datum'] = "{} {}".format(row['datum'][:-1], row['tijd'])
        if row['tijd']:
            row['datum'] = datetime.strptime(row['datum'], '%d/%m/%Y %H:%M')
        else:
            row['datum'] = datetime.strptime(row['datum'], '%d/%m/%Y ')
        del row['tijd']
        row['nummer'] = row.get('nummer','')
        row['datum'] = row.get('datum','')
        row['thuisploeg'] = row.get('thuisploeg','')
        row['bezoekers'] = row.get('bezoekers','')
        row['aantal_thuis'] = row.get('aantal_thuis','')
        row['aantal_bezoekers'] = row.get('aantal_bezoekers','')
        row['scheidsrechter'] = row.get('scheidsrechter','')
        row['jury'] = row.get('jury','')
        row['locatie'] = row.get('locatie','')
        row['reeks'] = row.get('reeks',reeks)
        return row
    return list(map(fix_row, d))


def make_table(lst):
    titles = ['nummer', 'reeks', 'datum','thuisploeg','bezoekers',
               'aantal_thuis', 'aantal_bezoekers', 'scheidsrechter', 'jury', 'locatie']
    cols = list(map(lambda x: [x[title] for title in titles], lst))
    return titles, cols

def parse_results(html, isclub=False, reeks=''):
    soup = BeautifulSoup(html, 'html.parser')
    lst = dict_from_table(soup, isclub=isclub)
    lst = fix_format(lst, reeks=reeks)
    titles, cols = make_table(lst)
    return titles, cols


def get_all(id):
    soup = BeautifulSoup(requests.get('http://korfbal.be/Wedstrijden/senioren/veld').text, 'html.parser')
    answer = dict()
    for i in soup.find(id = id).find_all('option'):
        answer[i.get('value')] = i.text
    return answer

def export_query(filename, isclub=False, **kwargs):
    titles, cols = parse_results(get_by(**kwargs), isclub=isclub)
    with open(filename,'w',newline='') as f:
        writer = csv.writer(f)
        writer.writerow(titles)
        writer.writerows(cols)


if __name__=='__main__':
    #all_blocks = get_all(id = 'cphMiddle_ddlBlock')
    #all_clubs = get_all(id = 'cphMiddle_ddlClub')
    all_leagues = get_all(id='cphMiddle_ddlLeague')
    all_rows = []
    for league_id, league_name in all_leagues.items():
        titles, rows = parse_results(get_by(league=league_id), isclub=False, reeks=league_name)
        all_rows.extend(rows)
    all_rows.sort(key=lambda x:x[0])
    with open('all_games.txt','w',newline='') as f:
        writer = csv.writer(f)
        writer.writerow(titles)
        writer.writerows(all_rows)
    '''
    print('Exporting clubs')
    for club_id, club_name in all_clubs.items():
        export_query(filename='data/clubs/{}.txt'.format(club_name
                                                          .replace(' ', '_')
                                                          .replace('/', '-')
                                                          .replace(':', '')),
                     club=club_id, isclub=True)

    print('Exporting blocks')
    for block_id, block_name in all_blocks.items():
        export_query(filename='data/blocks/{}.txt'.format(block_name
                                                          .replace(' ', '_')
                                                          .replace('/', '-')
                                                          .replace(':', '')),
                     block=block_id)

    print('Exporting leagues')
    for league_id, league_name in all_leagues.items():
        export_query(filename='data/leagues/{}.txt'.format(league_name
                                                          .replace(' ', '_')
                                                          .replace('/', '-')
                                                          .replace(':', '')),
                     league=league_id)
    print('DONE')
    '''


    '''
    Now we just have to add a way to get ALL games of a page (just go through every league)
    Maybe we could use pandas
    Don't forget the youth a
    '''