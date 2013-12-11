import urllib
#import xml.etree.ElementTree as ET
from bs4 import BeautifulSoup
CURRENT_ODDS_URL = r"http://www.footballlocks.com/nfl_odds.shtml"
HISTORICAL_WINS_URL = r"http://www.pro-football-reference.com/years/2012/games.htm"
import pandas as pd
import pdb
import os
import datetime as dt
from copy import deepcopy
import argparse
import pickle

def backfill_odds_data(startweek,endweek):
    today = dt.date.today()
    if os.path.exists("row_dicts" + today.strftime("%Y%m%d") + ".pickle"):
        print 'row dicts file already exists for today, skipping'
        return
    all_row_dicts = []
    for week in range(startweek,endweek+1):
        print 'running for week ' + str(week)
        url = "http://www.footballlocks.com/nfl_odds_week_" + str(week) + ".shtml"
        f = urllib.urlopen(url)
        html_code = f.read()
        html_code = html_code.replace("</D>","</TD>").replace('size=-1','').replace('SIZE=-1','')
        f.close()
        row_dicts = read_historical_tables_from_html(html_code)
        _add_week_num_to_dicts(row_dicts,week)
        all_row_dicts.extend(row_dicts)
    save_row_dicts(all_row_dicts)

def save_row_dicts(row_dicts):
    today = dt.date.today()
    f = open("row_dicts" + today.strftime("%Y%m%d") + ".pickle",'w')
    pickle.dump(row_dicts,f)
    f.close()

def new_make_current_picks(address = CURRENT_ODDS_URL):
    '''uses the same parsing engine, but on the current pick table'''
    f = urllib.urlopen(address)
    html_code = f.read()
    f.close()
    row_dicts = read_historical_tables_from_html(html_code)
    picks = produce_picks_from_rows(row_dicts)
    display_picks(picks)

def score_row(row_dict,method = 'fav'):
    #takes in a row dictionary
    #returns the team picked and probability of win for that team as a team,prob tuple
    team = row_dict['Favorite']
    odds_fav = row_dict['odds_favored']
    if odds_fav < 0:
        #should almost always be true
        odds = abs(odds_fav) / 100
        p = odds / (1 + odds)
    else:
        odds = 100 / odds_fav
        p = odds / (1 + odds) 
    pfav = p
    odds_underdog = row_dict['odds_underdog']
    if odds_underdog < 0:
        #should only happen in tossup games
        odds = abs(odds_underdog) / 100
        p = odds / (1 + odds)
    else:
        odds = 100 / odds_underdog
        p = odds / (1 + odds)
    pund = p
    pmid = (pfav + pund) / 2 #pretty sure this is garbage
    if method == 'fav':
        return team,pfav
    elif method == 'und':
        assert False
        return team,pund
    elif method == 'mid':
        assert False
        return team,pmid
    else:
        print 'unsupported method'

def _odds_to_prob(moneyline):
    if moneyline < 0:
        odds = abs(moneyline) / 100
        p = odds / (1+odds)
    else:
        odds = 100 / moneyline
        p = odds / ( 1 + odds )
    return p

def score_row_logit(row_dict,method='mid'):
    with open('logit_model.pickle') as f:
        logit_model = pickle.load(f)
    p_favored = (_odds_to_prob(row_dict['odds_favored']) + (1 - _odds_to_prob(row_dict['odds_underdog'])) ) / 2.0
    p_underdog = 1 - p_favored
    if row_dict['Underdog'] == row_dict['hometeam']:
        #underdog is hometeam
        home_data = {'ref_p':p_underdog,'is_at_home':1,'team_name':row_dict['hometeam'],'intercept':1}
        away_data = {'ref_p':p_favored,'is_at_home':0,'team_name':row_dict['awayteam'],'intercept':1}
    else:
        #underdog is awayteam
        home_data = {'ref_p':p_favored,'is_at_home':1,'team_name':row_dict['hometeam'],'intercept':1}
        away_data = {'ref_p':p_underdog,'is_at_home':0,'team_name':row_dict['awayteam'],'intercept':1}
    #home_df = pd.DataFrame(home_data)
    #away_df = pd.DataFrame(away_data)
    #home_df = [home_data['is_at_home'],home_data['ref_p']]
    #away_df = [away_data['is_at_home'],away_data['ref_p']]
    home_df = [home_data['is_at_home'],home_data['ref_p'],home_data['intercept']]
    away_df = [away_data['is_at_home'],away_data['ref_p'],away_data['intercept']]
    p_home = logit_model.predict(home_df)
    p_away = logit_model.predict(away_df)
    print 'home data'
    print str(home_data)
    print 'away data'
    print str(away_data)
    print "p_home is " + str(p_home) + " and p_away is " + str(p_away) + \
            ". hometeam is %s and away team is %s" %(home_data['team_name'],away_data['team_name'])
    if p_home > p_away:
        score = p_home
        team = home_data['team_name']
    else:
        score = p_away
        team = away_data['team_name']
    return team,score

def read_historical_tables_from_html(html_code):
    '''similar to reading the live odds, but designed for use in a one time backfill of historical odds
    takes in the html code of a website that contains multiple weeks worth of data
    returns a list of rows of data to be stored in a csv format or a database for later historical testing'''
    done = False
    #the original version of this code just reads in two tables, one for the main games and one in the separate
    #table for monday night football. this new version will loop through as many tables as it can find to get more and more data
    remaining_code = html_code[:] #necessary? not sure if passed by value or reference here
    all_results = []
    headers = None
    table_count = 0
    while not done:
        table_count += 1
        print 'table count is ' + str(table_count)
        results,new_index,headers = read_individual_table_code(remaining_code,pre_headers = headers)
        all_results.extend(results)
        if new_index == -1:
            done = True
        else:
            remaining_code = remaining_code[new_index:]
    return all_results

def get_last_year(html_code,tab_start_loc):
    '''returns the location of every year in the code'''
    years = range(2005,2030)
    found = False
    locs = []
    for year in years:
        loc = html_code.rfind(str(year),0,tab_start_loc)
        if loc != -1:
            locs.append((year,loc))
    if len(locs) == 0:
        print 'could not find a year'
        pdb.set_trace()
    else:
        locs.sort(key=lambda i:i[1]) #sort by the location. defaults to ascending so the last year is last in list
        return locs[-1][0]

def read_individual_table_code(remaining_code,pre_headers = []):
    '''takes in a sub piece of an html website
    returns extracted data from the first table it can find in the code and the index of the end of that table
    so that the next iteration can start off where this one left off'''
    tab_start_loc = remaining_code.find('<TABLE COLS="6"')
    if tab_start_loc == -1:
        '''in this case, the prior iteration was the last table. we couldn't find another table so report back that we're done
        we return an empty results set since it won't matter that that was appended'''
        return [],-1,None
    #end_table_loc = remaining_code[tab_start_loc+1:].find("</TABLE>")
    year = get_last_year(remaining_code,tab_start_loc)
    end_table_loc = remaining_code[tab_start_loc+1:].lower().find("</TABLE>".lower())
    table_html = remaining_code[tab_start_loc:tab_start_loc+end_table_loc+9] #9 is the length of the table txt itself
    next_index = tab_start_loc + end_table_loc + 9
    try:
        soup = BeautifulSoup(table_html)
    except:
        print 'trouble reading html code into soup'
        pdb.set_trace()
    headers = []
    found_headers = False
    row_dicts = [] #list of dicts where each item in list is a dict representing a row of data from table
    skip_first_row = False
    if len(soup.find_all('tr')) < 5:
        '''we're on a shortened mnf table with only one row, so reuse headers from before'''
        headers = pre_headers
        found_headers = True
        skip_first_row = True
    try:
        for row in soup.find_all('tr'):
            #each row is a bs4 tag that has a row of data
            if skip_first_row:
                '''helps us for the mnf table to skip the non header top row'''
                skip_first_row = False
                continue
            i = 0
            row_dict = {}
            for data_point in row.find_all('td'):
                content = data_point.text
                if type(content) == list:
                    if len(content) > 1:
                        print 'found a content list of length greater than 1. problem'
                        raise IOError('found a content list of length greater than 1')
                    else:
                        content = content[0]
                if found_headers:
                    row_dict[headers[i]] = content
                else:
                    headers.append(content.replace('\n','').replace('\r',''))
                i+=1
            if headers[0] == '':
                '''useful for the MNF games where they didn't include all the headers explicitly'''
                print 'WARNING'
                headers = pre_headers
            if found_headers:
                row_dicts.append(row_dict)
                '''
                if abs(float(row_dict['Spread']) - 5.0) < 0.25:
                    pdb.set_trace()
                '''
            found_headers = True
    except:
        import traceback
        traceback.print_exc()
    return enrich_row_dicts(row_dicts,year),next_index,headers


def enrich_row_dicts(row_dicts,year=dt.date.today().year):
    #takes in a list of dicts of field->value parsed from tables
    #adds additional field info to each row
    new_row_dicts = []
    for row_dict in row_dicts:
        new_row_dict = deepcopy(row_dict)
        try:
            #spread = float(row_dict['Spread'][3:])
            spread = float(row_dict['Spread'])
        except ValueError:
            spread = 0
        except:
            pdb.set_trace()
        new_row_dict['Spread'] = spread
        if abs(spread - 5.0) < .25:
            pdb.set_trace()
        #if row_dict['Underdog'][:2] == 'At':
        if row_dict['Underdog'].split(' ')[0] == 'At':
            #underdog is home team
            new_row_dict['hometeam'] = row_dict['Underdog'][3:]
            new_row_dict['awayteam'] = row_dict['Favorite']
            new_row_dict['Underdog'] = row_dict['Underdog'][3:]
            new_row_dict['Favorite'] = row_dict['Favorite']
        else:
            #underdog is away team
            new_row_dict['hometeam'] = row_dict['Favorite'][3:]
            new_row_dict['awayteam'] = row_dict['Underdog']
            new_row_dict['Underdog'] = row_dict['Underdog']
            new_row_dict['Favorite'] = row_dict['Favorite'][3:]
        try:
            spread_for,spread_against = map(_convert_money_line_to_float,row_dict['Money Odds'].strip().split(' '))
        except:
            pdb.set_trace()
            raise IOError("issue mapping to spread for against")
        new_row_dict['odds_favored'] = spread_for
        new_row_dict['odds_underdog'] = spread_against
        try:
            new_row_dict['Total'] = float(new_row_dict['Total'])
        except:
            new_row_dict['Total'] = 0.0
        try:
            if row_dict['Date & Time'].lower().startswith('postp'):
                fix_postponed(row_dict,new_row_dict)
            else:
                month,day = map(int,row_dict['Date & Time'].split(' ')[0].split('/'))
                if month == 1 and year==2009:
                    new_row_dict['full_datetime'] = dt.date(year+1,month,day) #catches cases where we've moved into jan
                else:
                    new_row_dict['full_datetime'] = dt.date(year,month,day)
        except:
            import traceback
            traceback.print_exc()
            print 'trouble handling date'
            print row_dict
            pdb.set_trace()
            new_row_dict['full_datetime'] = None
        new_row_dicts.append(new_row_dict)
    return new_row_dicts

def fix_postponed(row_dict,new_row_dict):
    '''need to fix date of game and re assign week number'''
    if new_row_dict['hometeam'] == 'Houston' and new_row_dict['awayteam'] == "Baltimore":
        new_row_dict['week_num'] = 10
        new_row_dict['full_datetime'] = dt.date(2008,11,9)
        print 'fixed a postponed game'
    elif new_row_dict['awayteam'] == "NY Giants" and new_row_dict['hometeam'] == "Minnesota":
        new_row_dict['full_datetime'] = dt.date(2010,12,13)
        print 'fixed a postponed game'
    elif new_row_dict['hometeam'] == "Philadelphia" and new_row_dict['awayteam'] == "Minnesota":
        new_row_dict['full_datetime'] = dt.date(2010,12,28)
        print 'fixed a postponed game'
    else:
        pdb.set_trace()

def display_picks(picks):
    #takes a sorted list of picks as (team,p of win) tuples
    #prints them out in nice form
    print "\n".join([str(teamname) + " with probability " + str(prob) for teamname,prob in picks])

def produce_picks_from_rows(row_dicts,use_logit=False):
    #takes a list a dicts of field -> value for each row in tables
    #extracts the relevant factors for predicting wins and returns ranked picks
    if not use_logit:
        results = map(score_row,row_dicts)
    else:
        results = map(score_row_logit,row_dicts)
    return sorted(results,key = lambda item:item[1],reverse=True)

def _convert_money_line_to_float(moneyline_string):
    #helper function to convert string of -$125 to the int -125
    try:
        x = float(moneyline_string.replace('$',''))
    except:
        x = -100
    return x

def _add_week_num_to_dicts(row_dicts,week_num):
    '''adds an extra field to each dict storing the week num'''
    for row_dict in row_dicts:
        if 'week_num' not in row_dict.keys():
            #don't overwrite if we already set it
            row_dict['week_num'] = week_num


def read_tables_from_html(html_code):
    #DEPRECATED
    #uses regex to parse 
    #table_re = r"<table>.*</table>"
    tab_start_loc = html_code.find('<TABLE COLS="6"')
    end_table_loc = html_code[tab_start_loc+1:].find("</TABLE>")
    first_table = html_code[tab_start_loc:tab_start_loc+end_table_loc+9] #9 is the length of the table txt itself
    table_substring = html_code[tab_start_loc+end_table_loc + 9:]
    second_table_start = table_substring.find('<TABLE COLS="6"')
    second_table_end = table_substring[second_table_start+1:].find("</TABLE>")
    second_table = table_substring[second_table_start:second_table_start + second_table_end + 9]
    soup1 = BeautifulSoup(first_table)
    soup2 = BeautifulSoup(second_table)
    headers = []
    found_headers = False
    row_dicts = [] #list of dicts where each item in list is a dict representing a row of data from table
    try:
        for row in soup1.find_all('tr'):
            #each row is a bs4 tag that has a row of data
            i = 0
            row_dict = {}
            for data_point in row.find_all('td'):
                content = data_point.text
                if type(content) == list:
                    if len(content) > 1:
                        print 'found a content list of length greater than 1. problem'
                        raise IOError("flkas")
                    else:
                        content = content[0]
                if found_headers:
                    row_dict[headers[i]] = content
                else:
                    headers.append(content.replace('\n','').replace('\r',''))
                i+=1
            if found_headers:
                row_dicts.append(row_dict)
            found_headers = True
    except:
        import traceback
        traceback.print_exc()
    found_headers = False
    for row in soup2.find_all('tr'):
        #for the monday night football game, which is in a separate table
        if not found_headers:
            found_headers = True
            continue
        i = 0
        row_dict = {}
        for data_point in row.find_all('td'):
            content = data_point.contents
            if type(content) == list:
                if len(content) > 1:
                    print 'found a content list of length greater than 1. problem'
                    raise IOError("found a content list of length greater than 1")
                else:
                    content = content[0]
            row_dict[headers[i]] = content
            i+=1
        row_dicts.append(row_dict)
    return enrich_row_dicts(row_dicts)

def read_current_odds(address = CURRENT_ODDS_URL,use_logit = False):
    #DEPRECATED
    f = urllib.urlopen(address)
    #f = urllib.urlopen("http://www.footballlocks.com/nfl_odds.shtml")
    html_code = f.read()
    f.close()
    #now convert it into a DOM
    #soup = BeautifulSoup(html_code)
    #t = tree.parse(html_code)
    row_dicts = read_tables_from_html(html_code) #returns a dictionary
    picks = produce_picks_from_rows(row_dicts,use_logit=use_logit)
    display_picks(picks)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="pick some teams!")
    parser.add_argument('--backfill',action='store_true',dest='backfill')
    parser.add_argument('--startweek',default=1,type=int)
    parser.add_argument('--endweek',default=17,type=int)
    parser.add_argument('--logit',action='store_true',dest='logit')
    args = parser.parse_args()
    if args.backfill:
        print 'backfilling data'
        backfill_odds_data(args.startweek,args.endweek)
    else:
        print 'running in regular weekly prediction mode'
        read_current_odds(use_logit = args.logit)
        #new_make_current_picks()
