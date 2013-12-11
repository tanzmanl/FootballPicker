import pandas as pd
from database_utils import query_all_games
from analyze_historical_data import _odds_to_prob, add_probability_columns
import datetime as dt
import pickle
import pdb

def old_score_row(row):
    '''takes in a row representing a historical game
    returns a team_ukey,score where the scores will be used for ranking
    score here is the simple offer for the favored team'''
    team = row['favorite_ukey']
    p = _odds_to_prob(row['odds_favored'])
    result = row['winner_ukey'] == team
    #return team,p,result
    return pd.Series({'chosen_team':team,'score':p,'result':result})

def mid_score_row(row):
    '''takes in a row representing a historical game
    returns a team_ukey,score where score will be used for ranking
    score here is based off of the midmarket odds'''
    team = row['favorite_ukey']
    p = (_odds_to_prob(row['odds_favored']) + (1 - _odds_to_prob(row['odds_underdog']))) / 2.0
    result = row['winner_ukey'] == team
    #return team,p,result
    return pd.Series({'chosen_team':team,'score':p,'result':result})

def mom_simple(row):
    '''scores row by always choosing the hometeam. score is the negative spread (so that the highest score
    means that the team is heavily favored) and then the negative negative spread when the hometeam is the underdog'''
    team = row['hometeam_ukey']
    if team == row['favorite_ukey']:
        score = -1.0 * row['spread']
    else:
        score = row['spread']
    result = row['winner_ukey'] == team
    #return team,score,result
    return pd.Series({'chosen_team':team,'score':score,'result':result})

def mom_threshold(row,threshold = 7):
    '''scores row by always choosing the hometeam, unless the spread is above some threshold, in which case we pick the 
    favorite team'''
    score = abs(row['spread'])
    if row['hometeam_ukey'] == row['favorite_ukey']:
        team = row['hometeam_ukey']
    else:
        if score > threshold:
            team = row['awayteam_ukey']
        else:
            team = row['hometeam_ukey']
    result = row['winner_ukey'] == team
    #return team,score,result
    return pd.Series({'chosen_team':team,'score':score,'result':result})

def score_row_logit(row):
    with open('logit_model.pickle') as f:
        logit_model = pickle.load(f)
    p_favored = (_odds_to_prob(row['odds_favored']) + (1 - _odds_to_prob(row['odds_underdog']))) / 2.0
    p_underdog = 1 - p_favored
    if row['underdog_ukey'] == row['hometeam_ukey']:
        home_data = {'ref_p':p_underdog,'is_at_home':1,'team_ukey':row['hometeam_ukey'],'intercept':1}
        away_data = {'ref_p':p_favored,'is_at_home':0,'team_ukey':row['awayteam_ukey'],'intercept':1}
    else:
        home_data = {'ref_p':p_favored,'is_at_home':1,'team_ukey':row['hometeam_ukey'],'intercept':1}
        away_data = {'ref_p':p_underdog,'is_at_home':0,'team_ukey':row['awayteam_ukey'],'intercept':1}

    #following part is model dependent on things like whether or now we added the intercept
    home_df = [home_data['is_at_home'],home_data['ref_p'],home_data['intercept']]
    away_df = [away_data['is_at_home'],away_data['ref_p'],away_data['intercept']]
    p_home = logit_model.predict(home_df)
    p_away = logit_model.predict(away_df)
    if p_home > p_away:
        score = p_home
        team = home_data['team_ukey']
    else:
        score = p_away
        team = away_data['team_ukey']
    result = row['winner_ukey'] == team
    return pd.Series({'chosen_team':team,'score':score,'result':result})

def backtest_results(df,scoring_function):
    '''takes in a dataframe of the complete historical results. goes year by year and week by week and scores the results
    of the games, returning some stats on the performance of the supplied scoring algorithm'''
    all_results = []
    for year in range(2006,2014):
        year_startpoint = dt.date(year,6,6).strftime("%Y-%m-%d")
        year_endpoint = dt.date(year+1,6,6).strftime("%Y-%m-%d")
        games_in_year = df[(df.date > year_startpoint) & (df.date < year_endpoint)]
        for week in range(1,18):
            this_weeks_games = games_in_year[games_in_year.week == week]
            if len(this_weeks_games) == 0:
                #print 'found an empty week for week %d in year %d ' % (week,year)
                continue
            #game_results = sorted(this_weeks_games.apply(scoring_function,axis=1),key = lambda item:item[1],reverse=True)
            game_df = this_weeks_games.apply(scoring_function,axis=1)
            num_games = len(this_weeks_games)
            MAX_POINTS = 16
            #points = pd.Series(range(MAX_POINTS,MAX_POINTS-num_games,-1))
            game_df.sort(columns='score',ascending=False,inplace=True)
            game_df['points'] = pd.Series(range(MAX_POINTS,MAX_POINTS-num_games,-1),index=game_df.index)
            game_df['earned_points'] = pd.Series(game_df['result'] * game_df['points'], index=game_df.index)
            weekly_score = game_df['earned_points'].sum()
            all_results.append((year,week,weekly_score))
    results_df = pd.DataFrame(all_results)
    results_df.columns = ["season","week","weekly_score"]
    return results_df

if __name__ == "__main__":
    df = query_all_games()
    print 'results for my old method'
    results_df = backtest_results(df,old_score_row)
    print 'average weekly score is %0.3f ' %results_df['weekly_score'].mean()
    print 'results for my old mid method'
    results_df = backtest_results(df,mid_score_row)
    print 'average weekly score is %0.3f ' %results_df['weekly_score'].mean()
    print 'results for mom simple method'
    results_df = backtest_results(df,mom_simple)
    print 'average weekly score is %0.3f ' %results_df['weekly_score'].mean()
    print 'results for mom threshold'
    results_df = backtest_results(df,mom_threshold)
    print 'average weekly score is %0.3f ' %results_df['weekly_score'].mean()
    print 'results for logit model'
    results_df = backtest_results(df,score_row_logit)
    print 'average weekly score is %0.3f ' %results_df['weekly_score'].mean()


