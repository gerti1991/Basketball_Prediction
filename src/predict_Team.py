import pandas as pd
import numpy as np
from pymongo import MongoClient
from connectors import mongo_connect,add_to_mongo
from datetime import datetime

try:
    events= mongo_connect('events_stats')
    TS = mongo_connect('team_stats')

    def add_prediction_columns(table1, table2, average_points=80, home_advantage=2.5):
        predictions = {'Event ID': [], 'Prediction_Away_Team': [], 'Prediction_Home_Team': []}

        # Iterate through each row in table1 to calculate predictions
        for index, row in table1.iterrows():
            if 'freeze' in row and row['freeze'] == True:  # Check if the 'freeze' column indicates to copy existing values
                predictions['Event ID'].append(row['Event ID'])
                predictions['Prediction_Away_Team'].append(row['Prediction_Away_Team'])  # Copy existing away team prediction
                predictions['Prediction_Home_Team'].append(row['Prediction_Home_Team'])  # Copy existing home team prediction
            else:
                # Find matching rows in table2 for away teams
                away_matches = table2[(table2['League'] == row['League']) & (table2['Team'] == row['Away'])]
                # Calculate Prediction_Away_Team for matching rows
                for _, away_row in away_matches.iterrows():
                    predictions['Event ID'].append(row['Event ID'])
                    predictions['Prediction_Away_Team'].append(away_row['Attacking Strength'] * away_row['Defensive Strength'] * away_row['Pace_league'] * average_points)
                    predictions['Prediction_Home_Team'].append(0)  # Placeholder for now

                # Find matching rows in table2 for home teams
                home_matches = table2[(table2['League'] == row['League']) & (table2['Team'] == row['Home'])]
                # Calculate Prediction_Home_Team for matching rows
                for _, home_row in home_matches.iterrows():
                    if row['Event ID'] in predictions['Event ID']:
                        # Update Prediction_Home_Team if Event ID already exists
                        idx = predictions['Event ID'].index(row['Event ID'])
                        predictions['Prediction_Home_Team'][idx] = home_advantage + home_row['Attacking Strength'] * home_row['Defensive Strength'] * home_row['Pace_league'] * average_points
                    else:
                        # Add new entry if Event ID doesn't exist
                        predictions['Event ID'].append(row['Event ID'])
                        predictions['Prediction_Away_Team'].append(0)  # Placeholder since it's a home match
                        predictions['Prediction_Home_Team'].append(home_advantage + home_row['Attacking Strength'] * home_row['Defensive Strength'] * home_row['Pace_league'] * average_points)

        # Convert predictions dictionary to DataFrame
        predictions_df = pd.DataFrame(predictions)
        predictions_df['Prediction_Home_Team'] = -1*( predictions_df['Prediction_Home_Team']- predictions_df['Prediction_Away_Team'])
        predictions_df['Prediction_Away_Team'] = -1* predictions_df['Prediction_Home_Team']
        # Merge predictions_df with table1 on 'Event ID'
        updated_table1 = pd.merge(table1, predictions_df[['Event ID', 'Prediction_Away_Team', 'Prediction_Home_Team']], on='Event ID', how='left')
        updated_table1.drop(['Prediction_Away_Team_x', 'Prediction_Home_Team_x'], axis=1, inplace=True)
        updated_table1.rename(columns={'Prediction_Away_Team_y': 'Prediction_Away_Team',
                                'Prediction_Home_Team_y': 'Prediction_Home_Team'}, inplace=True)
        return updated_table1

    updated_table1 = add_prediction_columns(events,TS)

    updated_table1['Prediction_Result_Team'] = np.where(updated_table1['Prediction_Home_Team']<updated_table1['Prediction_Away_Team'], 1, 0)
    updated_table1['Result_Spread_Team'] = np.where(updated_table1['Result']==0,(updated_table1['Away Score'] - updated_table1['Home Score'])+updated_table1['Prediction_Away_Team'],(updated_table1['Home Score'] - updated_table1['Away Score'])+updated_table1['Prediction_Home_Team'])
    updated_table1['Prediction_eff_Team'] = np.where((updated_table1['Prediction_Result_Team'] - updated_table1['Result'] == 0) & (updated_table1['Result_Spread_Team'] >= 0), 1, 0)

    events_stats = updated_table1
    data_dict_events_stats = events_stats.to_dict("records")
    add_to_mongo(data_dict_events_stats,'events_stats')
    print("Ok")

except Exception as e:
    print(f"Error: {e}")  # Print the error message if an exception occurs