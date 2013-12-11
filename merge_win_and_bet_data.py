import csv
import os
import datetime as dt
import pickle
from collections import namedtuple
import datetime
import pdb

wins_to_ukey = {
        'Indianapolis Colts':1,
        'New Orleans Saints':2,
        'Seattle Seahawks':3,
        'Tampa Bay Buccaneers':4,
        'Pittsburgh Steelers':5,
        'Cleveland Browns':6,
        'Green Bay Packers':7,
        'Philadelphia Eagles':8,
        'Tennessee Titans':9,
        'Jacksonville Jaguars':10,
        'Dallas Cowboys':11,
        'New York Giants':12,
        'New England Patriots':13,
        'New York Jets':14,
        'Minnesota Vikings':15,
        'Atlanta Falcons':16,
        'Washington Redskins':17,
        'Miami Dolphins':18,
        'Carolina Panthers':19,
        'St. Louis Rams':20,
        'San Diego Chargers':21,
        'Chicago Bears':22,
        'Denver Broncos':23,
        'Buffalo Bills':24,
        'Detroit Lions':25,
        'Oakland Raiders':26,
        'Houston Texans':27,
        'Kansas City Chiefs':28,
        'San Francisco 49ers':29,
        'Arizona Cardinals':30,
        'Cincinnati Bengals':31,
        'Baltimore Ravens':32
        }

odds_to_ukey = {
        'Indianapolis':1,
        'New Orleans':2,
        'Seattle':3,
        'Tampa Bay':4,
        'Pittsburgh':5,
        'tsburgh':5,
        'Cleveland':6,
        'Green Bay':7,
        'Philadelphia':8,
        'Philadephia':8,
        'Tennessee':9,
        'Jacksonville':10,
        'Dallas':11,
        'NY Giants':12,
        'Giants':12,
        'New England':13,
        'England':13,
        'NY Jets':14,
        'Jets':14,
        'Minnesota':15,
        'Atlanta':16,
        'Washington':17,
        'Miami':18,
        'Carolina':19,
        'St. Louis':20,
        'San Diego':21,
        'Diego':21,
        'Chicago':22,
        'cago':22,
        'Denver':23,
        'Buffalo':24,
        'Detroit':25,
        'Oakland':26,
        'Houston':27,
        'Kansas City':28,
        'San Francisco':29,
        'Francisco':29,
        'Arizona':30,
        'Cincinnati':31,
        'Baltimore':32
        }

months = {
        'August':8,
        'September':9,
        'October':10,
        'November':11,
        'December':12,
        'January':1,
        'February':2,
        'March':3
        }

def merge_to_unified_rows(wins_df = None):
    today = dt.date.today()
    odds_file_name = 'row_dicts' + today.strftime("%Y%m%d") + ".pickle"
    if wins_df is None:
        win_loss_data = read_csvs() #list of dicts
    else:
        win_loss_data = wins_df.transpose().to_dict()
    odds_data = read_pickle(odds_file_name) #list of dicts
    if len(odds_data) == 0:
        print 'no odds data read in, returning'
        return
    '''final target will be each game is represented as a dict
    it will have the fields date, week, favorite, underdog, winner, loser, hometeam, away team, odds favored,
    odds underdog, spread, total'''
    new_dicts = []
    '''loop through the odds data. that should be a subset of games of the win loss data'''
    unmatched = []
    count,tot = 0,len(odds_data)
    alt_count= 0
    for odds_row_dict in odds_data:
        '''try to match date and team ukeys'''
        count+=1
        print " %d of %d " %(count,tot)
        matches = []
        team1,team2,date = odds_row_dict['Underdog'].split('(')[0].strip(),odds_row_dict['Favorite'].split('(')[0].strip(),odds_row_dict['full_datetime']
        try:
            team1_ukey,team2_ukey = odds_to_ukey[team1],odds_to_ukey[team2]
        except:
            pdb.set_trace()
        if not isinstance(date,datetime.date):
            '''one of the three games missing a date?'''
            unmatched.append(odds_row_dict)
            alt_count+=1
            continue
        for win_loss_row_dict in win_loss_data.values():
            try:
                team1s,team2s,win_date = win_loss_row_dict['Winner/tie'],win_loss_row_dict['Loser/tie'],win_loss_row_dict['full_datetime']
            except:
                pdb.set_trace()
            t1_ukey,t2_ukey = wins_to_ukey[team1s],wins_to_ukey[team2s]
            ukey_set = set([t1_ukey,t2_ukey])
            if abs((win_date - date).days) <= 1 and team1_ukey in ukey_set and team2_ukey in ukey_set:
                '''we've found a match!'''
                matches.append(win_loss_row_dict)
                '''could stop here, but lets let it keep going for a bit to make sure we never double match'''
        win_loss_row_dict = None
        if len(matches) == 0:
            unmatched.append(odds_row_dict)
            if wins_df is None:
                #if we have passed it smaller subset of only the latest data, we don't need to worry that we've failed to match older data
                print 'len of matches is 0'
                pdb.set_trace()
        elif len(matches) > 1:
            print 'found more than one match for ' + str(odds_row_dict)
            print str(matches)
        else:
            '''we found exactly one match, which is ideal'''
            win_loss_row_dict = matches[0]
            team1s,team2s,win_date = win_loss_row_dict['Winner/tie'],win_loss_row_dict['Loser/tie'],win_loss_row_dict['full_datetime']
            t1_ukey,t2_ukey = wins_to_ukey[team1s],wins_to_ukey[team2s]
            new_dict = {
                    'date':win_date,
                    'week':odds_row_dict['week_num'],
                    'favorite':team2_ukey,
                    'underdog':team1_ukey,
                    'winner':t1_ukey,
                    'loser':t2_ukey,
                    'hometeam':odds_to_ukey[odds_row_dict['hometeam'].split('(')[0].strip()],
                    'awayteam':odds_to_ukey[odds_row_dict['awayteam'].split('(')[0].strip()],
                    'odds_favored':odds_row_dict['odds_favored'],
                    'odds_underdog':odds_row_dict['odds_underdog'],
                    'spread':odds_row_dict['Spread'],
                    'total':odds_row_dict['Total'],
                    'points_winner':int(win_loss_row_dict['PtsW']),
                    'points_loser':int(win_loss_row_dict['PtsL']),
                    'is_tie':win_loss_row_dict['PtsW'] == win_loss_row_dict['PtsL']
                    }
            new_dicts.append(new_dict)
    print 'number unmatched is ' + str(len(unmatched))
    print 'number alt count is %d ' % alt_count
    f = open('merged_data' + today.strftime('%Y%m%d') + '.pickle','w')
    pickle.dump(new_dicts,f)
    f.close()

def read_csvs():
    years = range(2004,2014)
    headers = []
    all_row_dicts = []
    for year in years:
        print 'reading ' + str(year)
        with open("winlossdata/" + str(year) + ".csv") as f:
            reader = csv.reader(f)
            for row in reader:
                if len(headers) == 0:
                    headers = row
                    continue
                if row[0] == "Week" or row[0] == '':
                    continue
                row_dict = {}
                for i in range(len(headers)):
                    row_dict[headers[i]] = row[i]
                day = row_dict['Date'].split(' ')[1]
                month_string = row_dict['Date'].split(' ')[0]
                month_num = months[month_string]
                if month_num < 6:
                    da_year = year+1
                else:
                    da_year = year
                row_dict['file_year'] = year
                row_dict['full_datetime'] = datetime.date(da_year,month_num,int(day))
                all_row_dicts.append(row_dict)
    return all_row_dicts

def read_pickle(fname = "row_dicts20131103.pickle"):
    print 'checking for file ' + fname
    if os.path.exists(fname):
        f = open(fname)
        rows = pickle.load(f)
        f.close()
    else:
        print 'file %s does not exist' % fname
        fs = sorted([f for f in os.listdir('.') if f.startswith('row_dicts')])
        if len(fs) == 0:
            return []
        print 'using %s instead' % fs[-1]
        f = open(fs[0])
        rows = pickle.load(f)
        f.close()
    return rows

if __name__ == "__main__":
    merge_to_unified_rows()

