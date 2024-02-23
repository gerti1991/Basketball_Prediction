import pandas as pd
import numpy as np
from pymongo import MongoClient
from connectors import mongo_connect,add_to_mongo
from datetime import datetime

# try:
events= mongo_connect('events_stats')
player_spread= mongo_connect('BPM_Player_Spread')
TS = mongo_connect('team_stats')

# print(player_spread[player_spread['Team'] == 'CB Granada'])

def filter_dataframe(df, league=None, season=None, team=None):
    if league is not None:
        df = df[df['League'] == league]
    if season is not None:
        df = df[df['Season'] == season]
    if team is not None:
        df = df[df['Team'] == team]
    return df

def nr_matches_(df, league=None, season=None):
    if league is not None:
        df = df[df['League'] == league]
    if season is not None:
        df = df[df['Season'] == season]
    nr_matches = (df['Team'].count()-1)*2
    return  nr_matches


def predict_match(FINAL_2, league, season, home_team, away_team, Book_Odds_home, Book_Odds_away):
    team_1 = filter_dataframe(FINAL_2, league, season, home_team)
    team_2 = filter_dataframe(FINAL_2, league, season, away_team)
    count1 = team_1[team_1['Missing']==False]['Player'].count()
    count2 = team_2[team_2['Missing']==False]['Player'].count()
    if count1 == 10:
        team_1 = team_1[team_1['Missing']==False]
    if count2 == 10:
        team_2 = team_2[team_2['Missing']==False]
    if count1 != 10:
        team_1 = team_1.sort_values(by='% MIN', ascending=False).head(10)
    if count2 != 10:
        team_2 = team_2.sort_values(by='% MIN', ascending=False).head(10)
    nr_matches = nr_matches_(TS, league, season)
    # Perform calculations for team_1
    team_1['ADJ MIN'] = team_1['% MIN']/team_1['% MIN'].sum()*200 #240 is 5 player playing 48 minutes in NBA but in European is 5x40 200
    team_1['Expected Possessions'] = team_1['ADJ MIN']*team_1['Possessions/Min']

    # Perform calculations for team_2
    team_2['ADJ MIN'] = team_2['% MIN']/team_2['% MIN'].sum()*200
    team_2['Expected Possessions'] = team_2['ADJ MIN']*team_2['Possessions/Min']

    # Average expected possessions for both teams
    Av_Expected_Possessions = (team_1['Expected Possessions'].sum() + team_2['Expected Possessions'].sum()) / 2

    # Adjust the team_1 and team_2 data with the average expected possessions
    team_1['On Court % ADJ'] = (team_1['ADJ MIN']/48) * Av_Expected_Possessions
    team_2['On Court % ADJ'] = (team_2['ADJ MIN']/48) * Av_Expected_Possessions

    # Recalculate BPM/Min based on the adjusted possessions
    team_1['BPM/Min'] = ((team_1['BPM']*(team_1['On Court % ADJ']/100))/1.2)/team_1['ADJ MIN']
    try:
        team_2['BPM/Min'] = ((team_2['BPM']*(team_2['On Court % ADJ']/100))/1.2)/team_2['ADJ MIN']
    except:
        print(f'{team_2['Team']}')

    # Update the Spread Value based on the new BPM/Min
    team_1['Spread Value'] = team_1['ADJ MIN']* team_1['BPM/Min']
    team_2['Spread Value'] = team_2['ADJ MIN']* team_2['BPM/Min']


    # Prepare the final DataFrame for team_1 and team_2
    df1 = pd.DataFrame({
        'Team': [home_team],
        'Raw Spread Value': [team_1['Spread Value'].sum()],
        'Expected Season Win %': [((team_1['Spread Value'].sum()*2.7)+41)/82],#34 has the spanish championship matches
        'Book Odds': [Book_Odds_home]
    })

    df2 = pd.DataFrame({
        'Team': [away_team],
        'Raw Spread Value': [team_2['Spread Value'].sum()],
        'Expected Season Win %': [((team_2['Spread Value'].sum()*2.7)+41)/82],
        'Book Odds': [Book_Odds_away]
    })

    # Calculate the remaining statistics

    df1['Transform %'] = df1['Expected Season Win %'] * (1 - df2['Expected Season Win %'].iloc[0])
    df2['Transform %'] = df2['Expected Season Win %'] * (1 - df1['Expected Season Win %'].iloc[0])
    df1['Game Win %'] = df1['Transform %'] / (df1['Transform %'] + df2['Transform %'].iloc[0]) #- 0.0914
    df2['Game Win %'] = df2['Transform %'] / (df2['Transform %'] + df1['Transform %'].iloc[0]) #+ 0.0914
    df1['Estimated Spread'] = ((df1['Game Win %']*82)-41)/-2.7
    df1['Fair Odds'] = 1 / df1['Game Win %']
    df1['EV+'] = (df1['Game Win %'] * df1['Book Odds'].iloc[0]) - 1
    df2['Estimated Spread'] = ((df2['Game Win %']*82)-41)/-2.7
    df2['Fair Odds'] = 1 / df2['Game Win %']
    df2['EV+'] = (df2['Game Win %'] * df2['Book Odds'].iloc[0]) - 1

    # Combine the final DataFrames for team_1 and team_2 into one
    final_df = pd.concat([df1, df2], keys=['Home', 'Away'])
    final_df = final_df.reset_index(level=0).rename(columns={'level_0': 'Location'})

    # Format the final DataFrame for display
    # final_df = final_df.style.format({
    #     'Expected Season Win %': '{:.2%}',
    #     'Transform %': '{:.2%}',
    #     'Game Win %': '{:.2%}',
    #     'EV+': '{:.2%}',
    #     'Estimated Spread': '{:+.2f}'
    # })
    # return team_1
    # return team_2
    return final_df



predictions = {
"Prediction_Home_BPM":[],
"Prediction_Away_BPM": [],
"Event ID":[]
# 'Update':[],
#  'freeze':[]

}


for index, row in events.iterrows():
    if 'freeze' in row and row['freeze'] == True:
        predictions["Prediction_Home_BPM"].append(row['Prediction_Home_BPM'])
        predictions["Prediction_Away_BPM"].append(row['Prediction_Away_BPM'])
        predictions["Event ID"].append(row['Event ID'])
        # predictions['Update'].append(row['Update'])
        # predictions['freeze'].append(row['freeze'])
        continue
    else:            
        # if row['Status'] == 'Finished':
        #     predictions['freeze'].append(True)
        #     predictions['Update'].append(datetime.now())
        # else:
        #     predictions['freeze'].append(False)
        #     predictions['Update'].append(datetime.now())
        # Extract home and away team names from the current row
        home_team_name = row['Home']
        away_team_name = row['Away']
        league = row['League']
        seasson = row['Season']

        # Call the predict_match function with the current teams
        final_df = predict_match(
            FINAL_2=player_spread,
            league=league,
            season=seasson,
            home_team=home_team_name,
            away_team=away_team_name,
            Book_Odds_home=1.85,  # You might want to dynamically retrieve these values if they are available in 'df'
            Book_Odds_away=2.06
        )

        # Assuming predict_match returns a DataFrame or Series with the prediction result,
        # we append the result to our list. If it returns something else, adjust accordingly.
        predictions["Prediction_Home_BPM"].append(final_df[final_df['Team'] == home_team_name]['Estimated Spread'].values[0])
        predictions["Prediction_Away_BPM"].append(final_df[final_df['Team'] == away_team_name]['Estimated Spread'].values[0])
        predictions["Event ID"].append(row['Event ID'])
            

predictions = pd.DataFrame(predictions)

events = events.merge(predictions,on="Event ID", how='left')
events['Result'] = np.where(events['Home Score']-events['Away Score'] > 0, 1, 0)

column = 'Prediction_Home_BPM_y'
if column in events.columns:
    events.drop(['Prediction_Home_BPM_x','Prediction_Away_BPM_x'], axis=1,inplace=True)
    # print(events.info())
    new_column_names = {'Prediction_Home_BPM_y':'Prediction_Home_BPM','Prediction_Away_BPM_y':'Prediction_Away_BPM'}
    events = events.rename(columns=new_column_names)

events['Prediction_Result_BPM'] = np.where(events['Prediction_Home_BPM']<events['Prediction_Away_BPM'], 1, 0)
events['Result_Spread_BPM'] = np.where(events['Result']==0,(events['Away Score'] - events['Home Score'])+events['Prediction_Away_BPM'],(events['Home Score'] - events['Away Score'])+events['Prediction_Home_BPM'])
events['Result_Spread_BPM2'] = (events['Home Score']-events['Away Score']) - (-1*events['Prediction_Home_BPM'])
events['Prediction_eff_BPM'] = np.where((events['Prediction_Result_BPM'] - events['Result'] == 0) & (events['Result_Spread_BPM'] >= 0), 1, 0)

events_stats = events
data_dict_events_stats = events_stats.to_dict("records")
add_to_mongo(data_dict_events_stats,'events_stats')
print("Ok")



# except Exception as e:
#     print(f"Error: {e}")  # Print the error message if an exception occurs