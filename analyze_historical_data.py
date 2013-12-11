import pandas as pd
import numpy as np
import statsmodels.api as sm
from database_utils import query_all_games, lookup_ukey, display_text_name
import pdb
import pickle

def add_probability_columns(df):
    #lets add a column that represents the risk neutral probability of winning beforehand of the team that won
    #need to essentially apply a function to each row of the data
    get_odds_winner = lambda row:row['odds_favored'] if row['favorite_ukey'] == row['winner_ukey'] else row['odds_underdog']
    get_odds_loser = lambda row:row['odds_underdog'] if row['favorite_ukey'] == row['winner_ukey'] else row['odds_favored']
    df['winner_odds'] = df.apply(get_odds_winner,axis=1)
    df['loser_odds'] = df.apply(get_odds_loser,axis=1)
    get_p_winner_bid = lambda row:_odds_to_prob(row['winner_odds'])
    get_p_winner_offer = lambda row: 1 - _odds_to_prob(row['loser_odds'])
    get_p_winner_mid = lambda row: (row['winner_p_bid'] + row['winner_p_offer']) / 2.0
    df['winner_p_bid'] = df.apply(get_p_winner_bid,axis=1)
    df['winner_p_offer'] = df.apply(get_p_winner_offer,axis=1)
    df['winner_p_mid'] = df.apply(get_p_winner_mid,axis=1)
    get_p_loser_bid = lambda row:_odds_to_prob(row['loser_odds'])
    get_p_loser_offer = lambda row: 1 - _odds_to_prob(row['winner_odds'])
    get_p_loser_mid = lambda row: (row['loser_p_bid'] + row['loser_p_offer']) / 2.0
    df['loser_p_bid'] = df.apply(get_p_loser_bid,axis=1)
    df['loser_p_offer'] = df.apply(get_p_loser_offer,axis=1)
    df['loser_p_mid'] = df.apply(get_p_loser_mid,axis=1)

def _odds_to_prob(moneyline):
    if moneyline < 0:
        odds = abs(moneyline) / 100
        p = odds / (1 + odds)
    else:
        odds = 100 / moneyline
        p = odds / ( 1 + odds ) 
    return p

def build_logit_model_individual(df):
    '''this is the big kahuna!
    takes in a dataframe object with all the historical parsed data pulled from the db
    it already should have probability values  ~ (0,1) computed from the odds listed
    it splits each game essentially into two trials, one using each team as the reference team
    so we should have nearly 4000 datapoints in our sample

    build a logit model where the input vector is 1/(1+e^(tx)) where tx = k0 * p for that team + k1 * is that the home team + (k2 to k34?) * which team is it
    possibly add another factor for what the opposing team is, and maybe a cross term factor of hometeam crossed with which team it is

    maybe include the spread?? very correlated with moneyline is likely to be an issue there
    '''
    dataset = get_home_team_ref(df).append(get_away_team_ref(df))
    dh = get_home_team_ref(df)
    da = get_away_team_ref(df)
    dnew = dh.append(da,ignore_index=True)
    dnew['intercept'] = 1.0
    print dnew.head()
    train_cols = ['is_at_home','ref_p','intercept']
    #train_cols = ['is_at_home','intercept']
    #train_cols = ['is_at_home','ref_p']
    #train_cols = ['intercept']
    logit = sm.Logit(dnew['ref_won'],dnew[train_cols])
    result = logit.fit()
    print result.summary()
    with open('logit_model.pickle','w') as f:
        pickle.dump(result,f)

def build_logit_model_rescaled(df):
    '''same as above, but rescales the ref_p to be exponential'''
    dh = get_home_team_ref_scaled(df)
    da = get_away_team_ref_scaled(df)
    dnew = dh.append(da,ignore_index=True)
    dnew['intercept'] = 1.0
    print dnew.head()
    train_cols = ['is_at_home','ref_p','intercept']
    #train_cols = ['is_at_home','intercept']
    #train_cols = ['is_at_home','ref_p']
    #train_cols = ['intercept']
    logit = sm.Logit(dnew['ref_won'],dnew[train_cols])
    result = logit.fit()
    print result.summary()
    with open('logit_model_scaled.pickle','w') as f:
        pickle.dump(result,f)


def get_home_team_ref(df):
    '''reconstructs a full dataset down to just the arguments we want to use for our regression
    this function extracts the rows from the reference point of the hometeam'''
    get_hometeam_p = lambda row:row['winner_p_mid'] if row['hometeam_ukey'] == row['winner_ukey'] else row['loser_p_mid']
    get_hometeam_won = lambda row: 1 if row['winner_ukey'] == row['hometeam_ukey'] else 0
    hometeam_p = df.apply(get_hometeam_p,axis=1)
    hometeam_won = df.apply(get_hometeam_won,axis=1)
    new_df = pd.DataFrame({'ref_p':hometeam_p,'ref_won':hometeam_won,'is_at_home':1})
    return new_df

def get_away_team_ref(df):
    '''reconstructs a full dataset down to just the arguments we want to use for our regression
    this function extracts the rows from the reference point of the awayteam'''
    get_awayteam_p = lambda row:row['winner_p_mid'] if row['awayteam_ukey'] == row['winner_ukey'] else row['loser_p_mid']
    get_awayteam_won = lambda row: 1 if row['winner_ukey'] == row['awayteam_ukey'] else 0
    awayteam_p = df.apply(get_awayteam_p,axis=1)
    awayteam_won = df.apply(get_awayteam_won,axis=1)
    new_df = pd.DataFrame({'ref_p':awayteam_p,'ref_won':awayteam_won,'is_at_home':0})
    return new_df

def _rescale_p(p):
    '''rescales p to be on exponential scale'''
    if p == 0:
        p = 0.000000000000000001
    if p == 1:
        p = 0.999999999999999999
    return -1.0 * np.log(1.0 / p - 1)

def get_home_team_ref_scaled(df):
    get_hometeam_p = lambda row:_rescale_p(row['winner_p_mid']) if row['hometeam_ukey'] == row['winner_ukey'] else _rescale_p(row['loser_p_mid'])
    get_hometeam_won = lambda row: 1 if row['winner_ukey'] == row['hometeam_ukey'] else 0
    hometeam_p = df.apply(get_hometeam_p,axis=1)
    hometeam_won = df.apply(get_hometeam_won,axis=1)
    new_df = pd.DataFrame({'ref_p':hometeam_p,'ref_won':hometeam_won,'is_at_home':1})
    return new_df

def get_away_team_ref_scaled(df):
    '''reconstructs a full dataset down to just the arguments we want to use for our regression
    this function extracts the rows from the reference point of the awayteam'''
    get_awayteam_p = lambda row:_rescale_p(row['winner_p_mid']) if row['awayteam_ukey'] == row['winner_ukey'] else _rescale_p(row['loser_p_mid'])
    get_awayteam_won = lambda row: 1 if row['winner_ukey'] == row['awayteam_ukey'] else 0
    awayteam_p = df.apply(get_awayteam_p,axis=1)
    awayteam_won = df.apply(get_awayteam_won,axis=1)
    new_df = pd.DataFrame({'ref_p':awayteam_p,'ref_won':awayteam_won,'is_at_home':0})
    return new_df

def build_logit_model_game(df):
    '''
    another possility is to not split each historical game into 2 samples, and try to regress with individual teams and have something of the form
    tx = k0 * market p of hometeam + k1 * which is hometeam + k2 * which is away team + maybe the cross term team x hometeam terms, and the p of this is like p that hometeam wins
    ideas for extension: demean the bets by team?
    '''
    pass


if __name__ == "__main__":
    df = query_all_games()
    add_probability_columns(df)
    #build_logit_model_individual(df)
    build_logit_model_rescaled(df)

