import pandas as pd
import numpy as np
from pymongo import MongoClient
from connectors import mongo_connect,add_to_mongo
from datetime import datetime
from scipy.stats import norm

betting_line_total_dic = {
    'Spanish-ACB':164.064587,
    'Italian-Lega-Basket-Serie-A':163.1583333,
    'Greek-HEBA-A1':155.9617117,
    'German-BBL':169.4248366,
    'French-Jeep-Elite':166.8300654,
    'Turkish-BSL':163.2833333,
    'Italian-Serie-A2-Basket':153.3642857,
    'Croatian-A-1-Liga':161.5035354,
    'Serbian-KLS':167.05

}


try:
    events= mongo_connect('events_stats')
    SP = mongo_connect('market_spreads')
    columns_to_drop = ['Money Line Home Odds', 'Away Odds (OT Inc)','Betting Line Total','Over Odds'
                                    , 'Under Odds','handicap line','home odds to beat handicap','away odds to beat handicap']

    events.drop(columns_to_drop,axis=1,inplace=True)

    events['Event ID'] =events['Event ID'].astype('string')

    merged = pd.merge(events,SP[['event_id','hdp_home']],left_on='Event ID',right_on='event_id',how='left')
    # df = merged[['Date','League','Home','Away','Home Score','Away Score','Prediction_Home_BPM','Prediction_Away_BPM','Prediction_Home_Market','Prediction_Away_Market','Prediction_Home_Team','Prediction_Away_Team','Prediction_Home_Universal','Prediction_Away_Universal','Points_Home_Universal','Points_Away_Universal','Total_Points_Universal','hdp_home','Status']]


    merged.drop('event_id',axis=1,inplace=True)
    merged = merged.rename(columns={'hdp_home': 'handicap line'})
    df = merged

    def round_to_nearest_half(num):
        sign = 1
        if num<0:
            sign=-1
        rounded_to_int = int(num)  # Round to the nearest integer
        remainder = num - rounded_to_int

        if abs(remainder) >= 0.25 and abs(remainder)<=0.5:
            return sign*(abs(rounded_to_int) + 0.5)
        elif abs(remainder)>0.5:
            return sign*(abs(rounded_to_int)+1)
        else:
            return rounded_to_int

    df['handicap line'] = df['handicap line'].fillna(0) 
    df['Prediction_Home_Universal'] = df['Prediction_Home_Universal'].fillna(0)
    df['Prediction_Home_Universal_'] = df['Prediction_Home_Universal'].apply(lambda x: round_to_nearest_half(x))
    df['handicap line'] = np.where(df['handicap line'].isin([0, '']), df['Prediction_Home_Universal_'], df['handicap line'])
    df2 = df



    df2['Total_Points'] = df2['Home Score'] + df2['Away Score']
    df2['Diff_Points'] = -1*(df2['Home Score'] - df2['Away Score'])
    df2__ = df2[df2['Status']=='Finished']
    unique_leagues = df2__['League'].unique()


    sdt = pd.DataFrame(columns=['League', 'SDEV_Total_Points', 'SDEV_Diff_Points','Betting_Line_Total'])
    for unique in unique_leagues:
        filtered_df2 = df2__[df2__['League'] == unique]
        std_total_points = filtered_df2['Total_Points'].std()
        std_diff_points = filtered_df2['Diff_Points'].std()
        Betting_Line_Total = filtered_df2['Total_Points'].mean()
        new_row = {'League': unique, 
                'SDEV_Total_Points': std_total_points, 
                'SDEV_Diff_Points': std_diff_points, 
                'Betting_Line_Total': Betting_Line_Total}
        sdt = pd.concat([sdt, pd.DataFrame([new_row])], ignore_index=True)
        # print(f"League {unique} is \n :{df2__[df2__['League']==unique][['Total_Points','Diff_Points']].std()}")
    sdt

    def calculate_odds(row, sdt_df,betting_line_total_dic):
        league = row['League']

        # Money Line Odds
        diff_points = row['Prediction_Home_Universal']
        handicap = row['handicap line']
        std_dev_diff = sdt_df.loc[sdt_df['League'] == league, 'SDEV_Diff_Points'].values[0]
        money_line_home_odds = 1 / norm.cdf(0, loc=diff_points, scale=std_dev_diff)
        home_odds_to_beat_handicap = 1 / norm.cdf(0, loc=handicap, scale=std_dev_diff)
        away_odds = 1 / (1 - (1 / money_line_home_odds))
        away_odds_to_beat_handicap = 1 / (1 - (1 / home_odds_to_beat_handicap))
        # Under/Over Odds
        betting_line_total = betting_line_total_dic[league]
        total_points = row['Total_Points_Universal']
        std_dev_total = sdt_df.loc[sdt_df['League'] == league, 'SDEV_Total_Points'].values[0]
        under_odds = 1 / norm.cdf(betting_line_total, loc=total_points, scale=std_dev_total)
        over_odds = 1 / (1 - (1 / under_odds))

        return {
            'Money Line Home Odds': money_line_home_odds,
            'Away Odds (OT Inc)': away_odds,
            'Betting Line Total':betting_line_total,
            'Under Odds': under_odds,
            'Over Odds': over_odds,
            'handicap line':handicap,
            'home odds to beat handicap':home_odds_to_beat_handicap,
            'away odds to beat handicap':away_odds_to_beat_handicap
        }

    results_df = pd.DataFrame(columns=['Event ID','Date','League', 'Home', 'Away', 'Home Score',
                                    'Away Score', 'Total_Points', 'Diff_Points','Points_Home_Universal','Points_Away_Universal','Total_Points_Universal','Prediction_Home_Universal',
                                    'Money Line Home Odds', 'Away Odds (OT Inc)','Betting Line Total','Over Odds'
                                    , 'Under Odds','handicap line','home odds to beat handicap','away odds to beat handicap','Status'])

    # Iterate through rows of df__
    for index, row in df.iterrows():
        # Get existing game data
        game_data = row[['Event ID','Date','League' ,'Home', 'Away', 'Home Score', 'Away Score', 'Total_Points', 'Diff_Points','Points_Home_Universal','Points_Away_Universal','Total_Points_Universal','Prediction_Home_Universal','handicap line','Status']].to_dict()

        # Calculate odds
        odds = calculate_odds(row, sdt,betting_line_total_dic)

        # Combine game data and odds
        complete_data = {**game_data, **odds}

        # Add a new row to the results DataFrame
        results_df = pd.concat([results_df, pd.DataFrame([complete_data])], ignore_index=True)

    merged = pd.merge(events,results_df[['Event ID','Money Line Home Odds', 'Away Odds (OT Inc)','Betting Line Total','Over Odds'
                                    , 'Under Odds','handicap line','home odds to beat handicap','away odds to beat handicap']],on='Event ID',how='left')
    events_stats = merged
    data_dict_events_stats = events_stats.to_dict("records")
    add_to_mongo(data_dict_events_stats,'events_stats')
    print("Ok")

except Exception as e:
    print(f"Error: {e}")  # Print the error message if an exception occurs