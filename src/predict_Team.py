import pandas as pd
import numpy as np
from pymongo import MongoClient
from connectors import mongo_connect,add_to_mongo
from datetime import datetime

try:
    events= mongo_connect('events_stats')
    TS = mongo_connect('team_stats')

    def add_prediction_columns(table1, table2):
        points = {
            'French-Jeep-Elite':{'average_points':82,'home_advantage':3.58},
            'Italian-Lega-Basket-Serie-A':{'average_points':80,'home_advantage':3.14},
            'Spanish-ACB':{'average_points':82,'home_advantage':3.28},
            'German-BBL':{'average_points':85,'home_advantage':1.73},
            'Greek-HEBA-A1':{'average_points':78,'home_advantage':4.29},
            'Turkish-BSL':{'average_points':82,'home_advantage':2.79},
            'Italian-Serie-A2-Basket':{'average_points':75,'home_advantage':3.22},
            'Croatian-A-1-Liga':{'average_points':81,'home_advantage':2.75},
            'Serbian-KLS':{'average_points':82,'home_advantage':4.41}
        }

        predictions = {'Event ID': [], 'Points_Away_Team': [], 'Points_Home_Team': [],'Points_Total_Team':[]}

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
            predictions['Points_Away_Team'].append(away_row['Attacking Strength'] * 
                                                    home_row['Defensive Strength'] * 
                                                    home_row['Pace_league'] * 
                                                    away_row['Pace_league'] *
                                                    points[row['League']]['average_points'])
            
            predictions['Points_Home_Team'].append ( points[row['League']]['home_advantage'] + \
                                                    home_row['Attacking Strength'] * \
                                                    away_row['Defensive Strength'] * \
                                                    away_row['Pace_league'] * \
                                                    home_row['Pace_league'] * \
                                                    points[row['League']]['average_points'])
            
            predictions['Points_Total_Team'].append(away_row['Attacking Strength'] * 
                                                    home_row['Defensive Strength'] * 
                                                    home_row['Pace_league'] * 
                                                    away_row['Pace_league'] *
                                                    points[row['League']]['average_points'] +\
                                                    points[row['League']]['home_advantage'] + \
                                                    home_row['Attacking Strength'] * \
                                                    away_row['Defensive Strength'] * \
                                                    away_row['Pace_league'] * \
                                                    home_row['Pace_league'] * \
                                                    points[row['League']]['average_points'])

               
        return predictions

    


    predictions_df = pd.DataFrame(add_prediction_columns(events,TS))

    if 'freeze' in events:
        # predictions_df['Points_Away_Team'] = np.where(events['freeze'] == False,predictions_df['Points_Away_Team'],events['Points_Away_Team'])
        # predictions_df['Points_Home_Team'] = np.where(events['freeze'] == False,predictions_df['Points_Home_Team'],events['Points_Home_Team'])
        # predictions_df['Points_Total_Team'] = np.where(events['freeze'] == False,predictions_df['Points_Total_Team'],events['Points_Total_Team'])

        predictions_df['Prediction_Home_Team'] = np.where(events['freeze'] == False,-1*( predictions_df['Points_Home_Team']- predictions_df['Points_Away_Team']),events['Prediction_Home_Team'])
        predictions_df['Prediction_Away_Team'] = np.where(events['freeze'] == False,-1* predictions_df['Prediction_Home_Team'],events['Prediction_Away_Team'])
    else:
        predictions_df['Prediction_Home_Team'] = -1*( predictions_df['Points_Home_Team']- predictions_df['Points_Away_Team'])
        predictions_df['Prediction_Away_Team'] = -1* predictions_df['Prediction_Home_Team']
    updated_table1 = pd.merge(events, predictions_df[['Event ID', 'Prediction_Away_Team', 'Prediction_Home_Team']], on='Event ID', how='left')
    updated_table1.drop(['Prediction_Away_Team_x', 'Prediction_Home_Team_x',], axis=1, inplace=True)
    updated_table1.rename(columns={'Prediction_Away_Team_y': 'Prediction_Away_Team',
                            'Prediction_Home_Team_y': 'Prediction_Home_Team'}, inplace=True)


    updated_table1['Prediction_Result_Team'] = np.where(updated_table1['Prediction_Home_Team']<updated_table1['Prediction_Away_Team'], 1, 0)
    updated_table1['Result_Spread_Team'] = np.where(updated_table1['Result']==0,(updated_table1['Away Score'] - updated_table1['Home Score'])+updated_table1['Prediction_Away_Team'],(updated_table1['Home Score'] - updated_table1['Away Score'])+updated_table1['Prediction_Home_Team'])
    updated_table1['Result_Spread_Team2'] = (updated_table1['Home Score'] - updated_table1['Away Score']) - (-1*updated_table1['Prediction_Home_Team'])
    updated_table1['Prediction_eff_Team'] = np.where((updated_table1['Prediction_Result_Team'] - updated_table1['Result'] == 0) & (updated_table1['Result_Spread_Team'] >= 0), 1, 0)
    updated_table1.drop('Updated',axis=1, inplace=True, errors='ignore')
    events_stats = updated_table1
    data_dict_events_stats = events_stats.to_dict("records")
    add_to_mongo(data_dict_events_stats,'events_stats')
    print("Ok")

except Exception as e:
    print(f"Error: {e}")  # Print the error message if an exception occurs