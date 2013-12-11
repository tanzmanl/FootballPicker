import sqlite3
from merge_win_and_bet_data import wins_to_ukey, odds_to_ukey
import pickle
import argparse
import pdb
import pandas as pd
import os

DATABASE_LOCATION = "database"
DB_NAME = "football_stats.db"
GAMES_PICKLE = "merged_data.pickle"

def create_database(database_location = DATABASE_LOCATION,db_name=DB_NAME):
    conn = sqlite3.connect(database_location + '/' + db_name)
    conn.close()

def update_games_database(merged_data_file = None):
    if merged_data_file is None:
        fs = sorted([f for f in os.listdir('.') if f.startswith('merged_data')])
        print 'using ' + str(fs[-1])
        merged_data_file = fs[-1]
    with open(merged_data_file) as f:
        all_data_dicts = pickle.load(f)
    conn = sqlite3.connect(DATABASE_LOCATION + '/' + DB_NAME)
    c = conn.cursor()
    count_updated = 0
    for game_dict in all_data_dicts:
        #check if the game is already in the table. if not, add it
        if not_in_table(game_dict,c):
            count_updated += 1
            sql = "insert into games values(%d,'%s',%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d)"%tuple([game_dict[k] for k in sorted(game_dict.keys())])
            c.execute(sql)
    print 'number of games added is ' + str(count_updated)
    conn.commit()
    conn.close()

def not_in_table(game_dict,c):
    '''checks if this game dict is already in the table. returns true if its not in table'''
    sql = ''' select 1 from games where date = '%s' and awayteam_ukey = %d''' % (game_dict['date'],game_dict['awayteam'])
    c.execute(sql)
    results = c.fetchall()
    return len(results) == 0

def add_games_database(create_tables=False,merged_data_file=GAMES_PICKLE):
    conn = sqlite3.connect(DATABASE_LOCATION + '/' + DB_NAME)
    c = conn.cursor()
    if create_tables:
        drop_table_sql = '''DROP TABLE games'''
        try:
            c.execute(drop_table_sql)
        except:
            print 'table did not already exist'
        create_table_sql = '''
        create table games (
        awayteam_ukey smallint, date text, favorite_ukey smallint, hometeam_ukey smallint,
        is_tie smallint, loser_ukey smallint, odds_favored real, odds_underdog real,
        points_loser smallint, points_winner smallint, spread real, total real,
        underdog_ukey smallint, week smallint, winner_ukey smallint
        )
        '''
        c.execute(create_table_sql)
    f = open(merged_data_file)
    all_data_dicts = pickle.load(f)
    f.close()
    #loop through each dict and create a sql line to add to the database
    for game_dict in all_data_dicts:
        sql = "insert into games values(%d,'%s',%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d)"%tuple([game_dict[k] for k in sorted(game_dict.keys())])
        c.execute(sql)
    conn.commit()
    conn.close()

def query_all_games(verbose=False):
    da_fields = ['awayteam_ukey','date','favorite_ukey','hometeam_ukey','is_tie','loser_ukey',\
            'odds_favored','odds_underdog','points_loser','points_winner','spread','total',\
            'underdog_ukey','week','winner_ukey']
    conn = sqlite3.connect(DATABASE_LOCATION + '/' + DB_NAME)
    c = conn.cursor()
    sql = 'select ' + ','.join(da_fields) + ' from games '
    c.execute(sql)
    results = c.fetchall()
    conn.close()
    df = pd.DataFrame(results)
    df.columns = da_fields
    return df

def add_team_name_lookup(database_location = DATABASE_LOCATION,db_name=DB_NAME,create_tables = False):
    conn = sqlite3.connect(database_location + '/' + db_name)
    c = conn.cursor()
    if create_tables:
        drop_table_sql = '''DROP TABLE wins_name_mapping'''
        drop_table_sql2 = ''' DROP TABLE odds_name_mapping'''
        c.execute(drop_table_sql)
        c.execute(drop_table_sql2)
        create_table_sql = '''
        CREATE TABLE wins_name_mapping (
         team_name text primary key, team_ukey smallint
        )
        '''
        c.execute(create_table_sql)
        create_table_sql_2 = '''
        CREATE TABLE odds_name_mapping (
        team_name text primary key, team_ukey smallint 
        )
        '''
        c.execute(create_table_sql_2)
    #now loop through all the data and add it to the table
    try:
        for key in wins_to_ukey:
            sql = "insert into wins_name_mapping values('%s',%d)" %(key,wins_to_ukey[key])
            print sql
            c.execute(sql)
        for key in odds_to_ukey:
            sql = "insert into odds_name_mapping values('%s',%d)" %(key,odds_to_ukey[key])
            print sql
            c.execute(sql)
    except:
        import traceback
        traceback.print_exc()
        pdb.set_trace()
    conn.commit()  
    conn.close()

def lookup_ukey(team_name,source='odds',verbose=False):
    '''source can be 'odds' or 'wins' '''
    conn = sqlite3.connect(DATABASE_LOCATION + '/' + DB_NAME)
    c = conn.cursor()
    sql = '''select team_ukey from %s_name_mapping where team_name = '%s' ''' % (source,team_name)
    if verbose:
        print sql
    c.execute(sql)
    results = c.fetchall()
    conn.close()
    if len(results) != 1:
        print 'bad name entered'
    else:
        return results[0][0]

def display_text_name(ukey,source='wins',verbose=False):
    '''source can be 'odds' or 'wins' '''
    conn = sqlite3.connect(DATABASE_LOCATION + '/' + DB_NAME)
    c = conn.cursor()
    sql = '''select team_name from %s_name_mapping where team_ukey = %d ''' %(source,ukey)
    if verbose:
        print sql
    c.execute(sql)
    results = c.fetchall()
    conn.close()
    if len(results) == 0:
        print 'found no match'
        return 'no match'
    else:
        return results[0][0] #could be multiple matches because of misspelling fixes

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description = "work with the database")
    parser.add_argument('--update',action='store_true',dest='update')
    args = parser.parse_args()
    if args.update:
        print 'updating games database'
        update_games_database()
    else:
        print 'no argument supplied'

