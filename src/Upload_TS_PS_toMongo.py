import requests
import pandas as pd
import numpy as np
from pymongo import MongoClient
from helpers import clean_and_convert,merge_rows_,calculate_team_average,calculate_sum_product,merge_rows
from connectors import mongo_connect,add_to_mongo


# Getting info from API
# URL and headers

# test API
# url_player_stats = "https://92ec-185-175-253-107.ngrok-free.app/api/v1/basketball/player-stats/Italy-Lega-A/2024/test-batch"
# url_team_stats = "https://92ec-185-175-253-107.ngrok-free.app/api/v1/basketball/team-stats/Italy-Lega-A/2024/test-batch"

# Production API links
url_player_stats = "https://possibly-brave-sailfish.ngrok-free.app/api/v1/basketball/player-stats/All/2024/test-batch"
url_team_stats = "https://possibly-brave-sailfish.ngrok-free.app/api/v1/basketball/team-stats/All/2024/test-batch"



url_events = "https://possibly-brave-sailfish.ngrok-free.app/api/v1/basketball/events/All/2024/test-batch"

headers = {
    'Auth-Key': 'F2RwH4EJ68Be2vS4WulSxnfQ',
    'Accept': 'application/json',
    'Content-Type': 'application/json'
}

try:
    # Get player stats
    response_player_stats = requests.get(url_player_stats, headers=headers)
    player_stats = response_player_stats.json()  # Extract JSON content

    # Get team stats
    response_team_stats = requests.get(url_team_stats, headers=headers)
    team_stats = response_team_stats.json()  # Extract JSON content

    #get events
    response_events = requests.get(url_events, headers=headers)
    events = response_events.json()  # Extract JSON content

    # Convert JSON content to pandas DataFrames
    PS = pd.DataFrame(player_stats)
    TS = pd.DataFrame(team_stats)
    events = pd.DataFrame(events)

    trim_func = lambda x: x.strip() if isinstance(x, str) else x
    PS = PS.applymap(trim_func)
    TS = TS.applymap(trim_func)
    events = events.applymap(trim_func)


    PS.columns = [[col.strip() for col in PS.columns]]
    TS.columns = [[col.strip() for col in TS.columns]]
    events.columns = [[col.strip() for col in events.columns]]

    PS.columns = [col[0] if isinstance(col, tuple) else col for col in PS.columns]
    TS.columns = [col[0] if isinstance(col, tuple) else col for col in TS.columns]
    events.columns = [col[0] if isinstance(col, tuple) else col for col in events.columns]

    PS = PS.drop_duplicates()
    TS = TS.drop_duplicates()
    events = events.drop_duplicates()

    # Assuming 'TS' is your DataFrame




    clean_and_convert(TS, ['League', 'Season', 'Team'])
    clean_and_convert(PS, ['League', 'Season', 'Team', 'Player', 'Position'])
    clean_and_convert(events, ['Date', 'Home', 'Away', 'League', 'Season','Link','Status'])

    # PS.drop('Efficiency Differential', axis=1, inplace=True)
    player_stats= PS
    team_stats= TS
    events_stats = events
    # Convert DataFrame to dictionary
    data_dict_player_stats = player_stats.to_dict("records")
    data_dict_team_stats = team_stats.to_dict("records")
    data_dict_events_stats = events_stats.to_dict("records")
    add_to_mongo(data_dict_player_stats,'player_stats')
    add_to_mongo(data_dict_team_stats,'team_stats')
    add_to_mongo(data_dict_events_stats,'events_stats')
    
    print("Ok")

except Exception as e:
    print(f"Error: {e}")  # Print the error message if an exception occurs


