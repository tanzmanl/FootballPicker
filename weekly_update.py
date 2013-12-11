import datetime as dt
from download_odds import backfill_odds_data
from merge_win_and_bet_data import merge_to_unified_rows
from database_utils import update_games_database
import pandas as pd
import pdb

import urllib
HISTORICAL_WINS_URL = r"http://www.pro-football-reference.com/years/%s/games.htm"
from bs4 import BeautifulSoup


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

def update_weekly_data():
    '''this is intended to be run once a week after all the games from the previous week have finished, so perhaps on tuesday or something
    if will go and download the latest odds data and create the odds dicts from that. it will also download the latest win data, merge the win data
    with the odds data, and then upload them to the database. its essentially just combining functionality from the other files into a simple
    update routine'''
    today = dt.date.today()
    print 'getting odds data'
    backfill_odds_data(1,17) # could probably be smarter about this and only grab the latest week. this downloads the odds data and stores it in a pickle
    print 'done getting odds data'
    print 'getting win loss data'
    df = download_latest_win_loss_data()
    print 'done downloading latest win loss data'
    print 'merging odds and wins data'
    merge_to_unified_rows(wins_df = df) #this reads in the wins from the csv files and creates a new merged csv
    print 'done merging data'
    print 'updating db'
    update_games_database() #this takes the merged data and updates the newest lines into the database
    print 'done updating db'
    print 'done updating for this week'

def download_latest_win_loss_data():
    #technically tries to upload all of the this seasons data
    today = dt.date.today()
    if today.month > 6:
        year = today.year
    else:
        year = today.year - 1
    my_url = HISTORICAL_WINS_URL % str(year)
    f = urllib.urlopen(my_url)
    html_code = f.read()
    f.close()
    table_start_loc = html_code.lower().find("<h2>week-by-week games")
    html_code = html_code[table_start_loc:]
    table_end_loc = html_code.lower().find("</table>") + 8
    tab_code = html_code[:table_end_loc]
    soup = BeautifulSoup(tab_code)
    headers = []
    all_rows = []
    for row in soup.find_all('tr'):
        #determine if its one of the header rows or a game row
        if len(row.find_all('td')) == 0:
            #it's a header row
            if len(row.find_all('th')) == 0:
                raise IOError("found a row that was neither headers nor data")
            headers = []
            for header in row.find_all('th'):
                headers.append(header.text)
        else:
            #we have table data items
            data_points = row.find_all('td')
            row_data = []
            for i in range(len(data_points)):
                row_data.append(data_points[i].text)
            if len(row_data) != len(headers):
                print 'length of data pulled from this row does not match length of latest headers row'
                pdb.set_trace()
            day = row_data[headers.index('Date')].split(' ')[1]
            month_string = row_data[headers.index('Date')].split(' ')[0]
            month_num = months[month_string]
            #year is already configured at the start of the function
            row_data.append(year)
            row_data.append(dt.date(year,month_num,int(day)))
            rr = tuple(row_data)
            all_rows.append(rr)
    headers = headers + ['file_year','full_datetime']
    df = pd.DataFrame(all_rows,columns=headers)
    return df

if __name__ == "__main__":
    update_weekly_data()
