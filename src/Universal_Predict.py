import pandas as pd
import numpy as np
from pymongo import MongoClient
from connectors import mongo_connect,add_to_mongo
from datetime import datetime


weights = {

'BPM': 0.4,
'Market': 0.5,
'Team': 0.1
}

# try:
#     events= mongo_connect('events_stats')

#     events['Prediction_Home_Universal'] = events['Prediction_Home_BPM']*weights['BPM']+events['Prediction_Home_Market']*weights['Market']+events['Prediction_Home_Team']*weights['Team']
#     events['Prediction_Away_Universal'] = events['Prediction_Away_BPM']*weights['BPM']+events['Prediction_Away_Market']*weights['Market']+events['Prediction_Away_Team']*weights['Team']

#     events['Prediction_Result_Universal'] = np.where(events['Prediction_Home_Universal']<events['Prediction_Away_Universal'], 1, 0)
#     events['Result_Spread_Universal'] = np.where(events['Result']==0,(events['Away Score'] - events['Home Score'])+events['Prediction_Away_Universal'],(events['Home Score'] - events['Away Score'])+events['Prediction_Home_Universal'])
#     events['Prediction_eff_Universal'] = np.where((events['Prediction_Result_Team'] - events['Result'] == 0) & (events['Result_Spread_Universal'] >= 0), 1, 0)


#     events_stats = events
#     data_dict_events_stats = events_stats.to_dict("records")
#     add_to_mongo(data_dict_events_stats,'events_stats')
#     print("Ok")

# except Exception as e:
#     print(f"Error: {e}")  # Print the error message if an exception occurs


try:
    events= mongo_connect('events_stats')
    market= mongo_connect('market_bet')
    BPM = mongo_connect('BPM_squad')
    teams = mongo_connect('team_stats')

    merged_df = pd.merge(market, BPM, how='inner',  left_on=['league_name', 'Team'], right_on=['League', 'Team'])
    merged_df.rename(columns={'Att_MIS_x': 'Att_MIS_Market', 'Def_MIS_x': 'Def_MIS_Market','Att_MIS_y':'Att_MIS_BPM','Def_MIS_y':'Def_MIS_BPM'}, inplace=True)
    # Select desired columns
    result_df = merged_df[['League','Team','Att_MIS_Market', 'Def_MIS_Market','Att_MIS_BPM','Def_MIS_BPM']]  # Note the '_y' suffix if columns names clash
    merged_df = pd.merge(result_df, teams, how='inner',  on=['League', 'Team'])
    result_df = merged_df[['League','Team','Att_MIS_Market', 'Def_MIS_Market','Att_MIS_BPM','Def_MIS_BPM','Attacking Strength','Defensive Strength','Pace_league']]  # Note the '_y' suffix if columns names clash

    # print(result_df)

    def add_prediction_columns(table1, table2):
        points = {
            'French-Jeep-Elite':{'average_points':82,'home_advantage':3.58},
            'Italian-Lega-Basket-Serie-A':{'average_points':80,'home_advantage':3.14},
            'Spanish-ACB':{'average_points':82,'home_advantage':3.28},
            'German-BBL':{'average_points':85,'home_advantage':1.73},
            'Greek-HEBA-A1':{'average_points':78,'home_advantage':4.29}
        }

        predictions = {'Event ID': [], 'Points_Away_Universal': [], 'Points_Home_Universal': []}

        # Iterate through each row in table1 to calculate predictions
        for index, row in table1.iterrows():
            # Find matching rows in table2 for away teams
            away_matches = table2[(table2['League'] == row['League']) & (table2['Team'] == row['Away'])]

            # Find matching rows in table2 for home teams
            home_matches = table2[(table2['League'] == row['League']) & (table2['Team'] == row['Home'])]

            # Ensure team statistics are found correctly
            if len(away_matches) == 0 or len(home_matches) == 0:
                print(f"Warning: Could not find team statistics for Event ID {row['Event ID']}")
                continue  # Move to the next game if data is missing

            # Access values from the FIRST matching row (since you should likely only have one unique entry per Team-League).
            away_row = away_matches.iloc[0] 
            home_row = home_matches.iloc[0] 

            # Update Away Team Prediction (Based on your provided calculation) 
            predictions['Event ID'].append(row['Event ID'])
            predictions['Points_Away_Universal'].append(away_row['Att_MIS_Market'] * 
                                                    home_row['Def_MIS_Market'] * 
                                                    away_row['Att_MIS_BPM'] * 
                                                    home_row['Def_MIS_BPM'] *
                                                    away_row['Pace_league'] *
                                                    home_row['Pace_league'] * 
                                                    points[row['League']]['average_points']*(1/1.02))
            
            predictions['Points_Home_Universal'].append ( home_row['Att_MIS_Market'] * 
                                                    away_row['Def_MIS_Market'] * 
                                                    home_row['Att_MIS_BPM'] * 
                                                    away_row['Def_MIS_BPM'] *
                                                    home_row['Pace_league'] *
                                                    away_row['Pace_league'] * 
                                                    points[row['League']]['average_points']*1.02)
            

            
        return predictions

    predictions_df = pd.DataFrame(add_prediction_columns(events,result_df))
    
    if 'freeze' in events:
        predictions_df['Points_Away_Team'] = np.where(events['freeze'] == False,predictions_df['Points_Away_Universal'],events['Points_Away_Universal'])
        predictions_df['Points_Home_Team'] = np.where(events['freeze'] == False,predictions_df['Points_Home_Universal'],events['Points_Home_Universal'])
        predictions_df['Total_Points_Universal'] = np.where(events['freeze'] == False,predictions_df['Points_Home_Universal'] + predictions_df['Points_Away_Universal'],events['Total_Points_Universal'])

        predictions_df['Prediction_Home_Universal'] = np.where(events['freeze'] == False,-1*( predictions_df['Points_Home_Universal']- predictions_df['Points_Away_Universal']),events['Prediction_Home_Universal'])
        predictions_df['Prediction_Away_Universal'] = np.where(events['freeze'] == False,-1* predictions_df['Prediction_Home_Universal'],events['Prediction_Away_Universal'])
    else:
        predictions_df['Total_Points_Universal'] = predictions_df['Points_Home_Universal'] + predictions_df['Points_Away_Universal']
        predictions_df['Prediction_Home_Universal'] = -1*( predictions_df['Points_Home_Universal']- predictions_df['Points_Away_Universal'])
        predictions_df['Prediction_Away_Universal'] = -1* predictions_df['Prediction_Home_Universal']
    # print(predictions_df[predictions_df['Event ID']==446542])
    updated_table1 = pd.merge(events, predictions_df[['Event ID', 'Prediction_Away_Universal', 'Prediction_Home_Universal','Points_Home_Universal','Points_Away_Universal','Total_Points_Universal']], on='Event ID', how='left')
    # print(updated_table1.info())
    updated_table1.drop(['Prediction_Away_Universal_x', 'Prediction_Home_Universal_x','Points_Home_Universal_x','Points_Away_Universal_x','Total_Points_Universal_x'], axis=1, inplace=True, errors='ignore')
    updated_table1.rename(columns={'Prediction_Away_Universal_y': 'Prediction_Away_Universal',
                            'Prediction_Home_Universal_y': 'Prediction_Home_Universal',
                            'Points_Away_Universal_y':'Points_Away_Universal','Points_Home_Universal_y':'Points_Home_Universal','Total_Points_Universal_y':'Total_Points_Universal'}, inplace=True)


    updated_table1['Prediction_Result_Universal'] = np.where(updated_table1['Prediction_Home_Universal']<updated_table1['Prediction_Away_Universal'], 1, 0)
    updated_table1['Result_Spread_Universal'] = np.where(updated_table1['Result']==0,(updated_table1['Away Score'] - updated_table1['Home Score'])+updated_table1['Prediction_Away_Universal'],(updated_table1['Home Score'] - updated_table1['Away Score'])+updated_table1['Prediction_Home_Universal'])
    updated_table1['Result_Spread_Universal2'] = (updated_table1['Home Score']-updated_table1['Away Score'])-(-1*updated_table1['Prediction_Home_Universal'])
    updated_table1['Prediction_eff_Universal'] = np.where((updated_table1['Prediction_Result_Universal'] - updated_table1['Result'] == 0) & (updated_table1['Result_Spread_Universal'] >= 0), 1, 0)
    # print(updated_table1.info())
    # print(updated_table1[updated_table1['Event ID']==446542][['Event ID','Points_Away_Universal','Points_Home_Universal','Total_Points_Universal','Prediction_Home_Universal','Prediction_Away_Universal']])
    events_stats = updated_table1
    data_dict_events_stats = events_stats.to_dict("records")
    add_to_mongo(data_dict_events_stats,'events_stats')
    print("Ok")

except Exception as e:
    print(f"Error: {e}")  # Print the error message if an exception occurs