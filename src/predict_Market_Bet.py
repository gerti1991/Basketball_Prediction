import requests
import json
import pandas as pd
from scipy.optimize import minimize
import numpy as np
import concurrent.futures
from connectors import mongo_connect,add_to_mongo

url = "https://possibly-brave-sailfish.ngrok-free.app/api/v1/basketball/events/closing-spreads/"

payload={}
files={}
headers = {
  'Auth-Key': 'F2RwH4EJ68Be2vS4WulSxnfQ',
  'Accept': 'application/json',
  'Content-Type': 'application/json'
}

try:
    response = requests.request("GET", url, headers=headers, data=payload, files=files)
    data = response.json()

    df = pd.DataFrame(data)


    unique_home_leagues = df['league_name'].unique().tolist()
    supremacy_forcast = {'league_name':[],'Team':[],'Ratings':[]}

    for league in unique_home_leagues:
        unique_home_teams = df[df['league_name']==league] ['home'].unique().tolist()
        for team in unique_home_teams:
            supremacy_forcast['league_name'].append(league)
            supremacy_forcast['Team'].append(team)
            supremacy_forcast['Ratings'].append(0)

    df2 = pd.DataFrame(supremacy_forcast)


    # Assuming your existing DataFrame is named df

    # Function to calculate rounds for each group
    def calculate_rounds(group):
        unique_teams_count = len(group['home'].unique())
        if unique_teams_count % 2 == 0:  # Even number of teams
            number_of_matches_per_round = unique_teams_count // 2
        else:  # Odd number of teams
            number_of_matches_per_round = (unique_teams_count - 1) // 2

        group['Round'] = group.index // number_of_matches_per_round
        return group

    # Apply the function to each league group
    df = df.groupby('league_name').apply(calculate_rounds).reset_index(drop=True)

    df = df.merge(df2.rename(columns={'Team': 'home', 'Ratings': 'Home_Rating'}), on=['home','league_name'], how='left')
    df = df.merge(df2.rename(columns={'Team': 'away', 'Ratings': 'Away_Rating'}), on=['away','league_name'], how='left')
    home = 2.5
    df['Forecast'] = home + df['Home_Rating'] - df['Away_Rating']
    df['Error'] = (-df['hdp_home'])-df['Forecast']
    df['weight'] = 1/(1+df['Round'])
    df['Squared error'] = (df['Error']*df['Error'])*df['weight']

    mean_errors = df['Squared error'].sum()

    # Function to calculate mean errors for each league
    def calculate_mean_errors_per_league(df, df2, league):
        # Filter dataframes for the current league
        df_league = df[df['league_name'] == league]
        df2_league = df2[df2['league_name'] == league]

        # Get teams and initial ratings for the current league
        teams = df2_league['Team'].tolist()
        initial_ratings = df2_league['Ratings'].values

        # Function to calculate mean errors
        def calculate_mean_errors(ratings_array):
            # Update ratings in df_league for each team
            ratings_dict = dict(zip(teams, ratings_array))
            for team, rating in ratings_dict.items():
                df_league.loc[df_league['home'] == team, 'Home_Rating'] = rating
                df_league.loc[df_league['away'] == team, 'Away_Rating'] = rating

            # Calculate Forecast, Error, weight, and Squared error
            df_league['Forecast'] = home + df_league['Home_Rating'] - df_league['Away_Rating']
            df_league['Error'] = (-df_league['hdp_home']) - df_league['Forecast']
            df_league['weight'] = 1 / (1 + df_league['Round'])
            df_league['Squared error'] = (df_league['Error'] * df_league['Error']) * df_league['weight']

            # Return the sum of Squared errors
            return df_league['Squared error'].sum()

        # Perform the optimization
        result = minimize(calculate_mean_errors, initial_ratings, method='SLSQP')
        return result.x

    def process_league(league):
        optimized_ratings = calculate_mean_errors_per_league(df, df2, league)

        # Update df2 with optimized ratings for the current league
        df2_league = df2[df2['league_name'] == league]
        for i, team in enumerate(df2_league['Team']):
            df2.loc[(df2['Team'] == team) & (df2['league_name'] == league), 'Ratings'] = optimized_ratings[i]

    # List of unique leagues
    leagues = df['league_name'].unique()

    # Use ThreadPoolExecutor to manage threads
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        # Submit tasks to the executor
        futures = [executor.submit(process_league, league) for league in leagues]

        # Wait for all threads to complete (optional if you need to do something after all threads are done)
        concurrent.futures.wait(futures)



    data_dict_market_bet = df2.to_dict("records")
    add_to_mongo(data_dict_market_bet,'market_bet')

    # print(df2[df2['league_name']=='Italian-Lega-Basket-Serie-A'])

    try:
        events= mongo_connect('events_stats')
        events = events.merge(df2, left_on=['League', 'Home'], right_on=['league_name', 'Team'], how='left')
        events.rename(columns={'Ratings': 'Home_Ratings'}, inplace=True)
        # Merge for Away Teams
        events = events.merge(df2, left_on=['League', 'Away'], right_on=['league_name', 'Team'], how='left')
        events.rename(columns={'Ratings': 'Away_Ratings'}, inplace=True)
        if 'freeze' in events:
            events['Prediction_Home_Market'] = np.where(events['freeze'] == False,(events['Home_Ratings'] + 2.5 - events['Away_Ratings'])*-1,events['Prediction_Home_Market'])
            events['Prediction_Away_Market'] = np.where(events['freeze'] == False,(events['Prediction_Home_Market'])*-1,events['Prediction_Away_Market'])
        else:
            events['Prediction_Home_Market'] = (events['Home_Ratings'] + 2.5 - events['Away_Ratings'])*-1
            events['Prediction_Away_Market'] = (events['Prediction_Home_Market'])*-1
        events['Prediction_Result_Market'] = np.where(events['Prediction_Home_Market']<events['Prediction_Away_Market'], 1, 0)
        events['Result_Spread_Market'] = np.where(events['Result']==0,(events['Away Score'] - events['Home Score'])+events['Prediction_Away_Market'],(events['Home Score'] - events['Away Score'])+events['Prediction_Home_Market'])
        events['Prediction_eff_Market'] = np.where((events['Prediction_Result_Market'] - events['Result'] == 0) & (events['Result_Spread_Market'] >= 0), 1, 0)
        events.drop(['Home_Ratings','Away_Ratings','Team_x','Team_y','league_name_x','league_name_y'], axis=1,inplace=True)
        events_stats = events
        data_dict_events_stats = events_stats.to_dict("records")
        add_to_mongo(data_dict_events_stats,'events_stats')

    except Exception as e:
        print(f"An error occurred: {e}")
        pass
    print("Ok")
except Exception as e:
    print(f"Error: {e}")  # Print the error message if an exception occurs