from fastapi import FastAPI, HTTPException, Request
from pymongo import MongoClient,UpdateOne
from fastapi.encoders import jsonable_encoder
import numpy as np
from fastapi import HTTPException
from bson import ObjectId
import logging
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
from fastapi.encoders import jsonable_encoder
from typing import List, Dict
from pydantic import BaseModel, Field
import pymongo
import subprocess
import logging
import os
from starlette.requests import Request
from starlette.responses import JSONResponse

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],  # Add the origin of your React app
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE"],  # Add the HTTP methods allowed
    allow_headers=["*"],  # You can restrict this to specific headers if needed
)


# Connect to MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client.Basketball  # Database name
dbhs = client.Historical_Data

events_stats_collection = db.events_stats
events_statsv2_collection = db.events_statsV2

market_bet_collection = db.market_bet
market_spreads_collection = db.market_spreads
team_stats_collection = db.team_stats
BPM_squad_collection = db.BPM_squad
events_stats_collection2 = db.BPM_Player
BPM_Player_Spread_collection = db.BPM_Player_Spread
market_bet_his_collection = dbhs.market_bet_his
BPM_squad_his_collection = dbhs.BPM_squad_his
Teams_his_collection = dbhs.Teams_his


async def update_trader_data(request_data: list):  # Expect a list of dicts
    # print(request_data)
    updates = []
    for item in request_data:
        try:
            _id = ObjectId(item['_id'])  # Convert string _id to ObjectId
        except ValueError:
            return {"error": "Invalid _id format in item: {}".format(item)}

        # 'trader rating' conversion 
        if 'trader_rating' in item:
            try:
                trader_rating = float(item['trader_rating'])
            except ValueError:
                trader_rating = None  # Set to None if not numeric

        # 'Missing' conversion
        if 'Missing' in item:
            missing = item['Missing']
        updates.append(
            pymongo.UpdateOne(
                {"_id": _id},
                {"$set": {
                    "trader rating": trader_rating,
                    "Missing": missing,
                }}
            )
        )
    if updates:
        result = events_stats_collection2.bulk_write(updates)
        return {"message": f"Updated {result.modified_count} documents"}
    else:
        return {"message": "No updates to perform"}


@app.get("/")
def read_root():
    return {"Hello": "World"}

# @app.get("/events-stats")
# def get_events_stats():
#     events = list(events_stats_collection.find({}, {"_id": 0}))
#     events = jsonable_encoder(events, custom_encoder={float: lambda x: 0 if np.isnan(x) else x})
#     return events


@app.get("/events-stats")
def get_events_stats(start: str = None, end: str = None,type=None):#,type=None
    query = {}  # Start with an empty query

    if start and end:
        # Filter 2: Between start and end
        query["Date"] = {"$gte": start, "$lte": end}

    elif start:
        # Filter 1: Greater than or equal to start
        query["Date"] = {"$gte": start} 

    elif end:
        # Filter 3: Less than or equal to end 
        query["Date"] = {"$lte": end} 
    if type =='0':
        query['Status'] = 'Finished'
    elif type =='1':
        query['Status'] = 'Scheduled'

    print(query)
    events = list(events_stats_collection.find(query, {"_id": 0}))



    events = jsonable_encoder(events, custom_encoder={float: lambda x: 0 if np.isnan(x) else x})
    return events

@app.get("/BPM_Player")
def get_bpm_players():
    players = list(events_stats_collection2.find({}))
    players_encoded = []
    for player in players:
        player_id = str(player.pop('_id'))  # Remove and store the _id field
        player_encoded = {key: (0 if isinstance(value, float) and pd.isna(value) else value) for key, value in player.items()}
        player_encoded['_id'] = player_id  # Add the _id field back to the player
        players_encoded.append(player_encoded)
    return players_encoded

# @app.put("/BPM_Player/{player_id}")
# def update_player_fields(player_id: str, updated_data: dict):
#     try:
#         # Convert player_id string to ObjectId
#         player_id = ObjectId(player_id)
#     except Exception as e:
#         raise HTTPException(status_code=400, detail="Invalid player ID format")

#     # Check if the player exists
#     player = events_stats_collection2.find_one({"_id": player_id})
#     if player:
#         # Update the specified fields from the request body
#         if updated_data:
#             events_stats_collection2.update_one({"_id": player_id}, {"$set": updated_data})
#             return {"message": "Player fields updated successfully."}
#         else:
#             return {"message": "No fields provided for update."}
#     else:
#         raise HTTPException(status_code=404, detail="Player not found")
    
@app.put("/BPM_Player")  # No more player_id path parameter
async def update_traders(request: Request):
    request_data = await request.json()
    result = await update_trader_data(request_data)
    return result 

@app.get("/market_bet")
def get_market_bet():
    bets = list(market_bet_collection.find({}, {"_id": 0}))
    bets = jsonable_encoder(bets, custom_encoder={float: lambda x: 0 if np.isnan(x) else x})
    return bets

@app.get("/market_spreads")
def get_market_spreads():
    spreads = list(market_spreads_collection.find({}, {"_id": 0}))
    spreads = jsonable_encoder(spreads, custom_encoder={float: lambda x: 0 if np.isnan(x) else x})
    return spreads

@app.get("/team_stats")
def get_team_stats():
    stats = list(team_stats_collection.find({}, {"_id": 0}))
    stats = jsonable_encoder(stats, custom_encoder={float: lambda x: 0 if np.isnan(x) else x})
    return stats

@app.get("/BPM_squad")
def get_bpm_squad():
    squad = list(BPM_squad_collection.find({}, {"_id": 0}))
    squad = jsonable_encoder(squad, custom_encoder={float: lambda x: 0 if np.isnan(x) else x})
    return squad


@app.get("/BPM_Player_Spread")
def get_bpm_squad():
    squad = list(BPM_Player_Spread_collection.find({}, {"_id": 0}))
    squad = jsonable_encoder(squad, custom_encoder={float: lambda x: 0 if np.isnan(x) else x})
    return squad


# @app.get("/market_bet_his")
# def get_events_stats():
#     events = list(market_bet_his_collection.find({}, {"_id": 0}))
#     events = jsonable_encoder(events, custom_encoder={float: lambda x: 0 if np.isnan(x) else x})
#     return events


@app.get("/market_bet_his")
def get_events_stats(start_week: str = None, end_week: str = None):
    query = {}  # Start with an empty query

    if start_week and end_week:
        # Filter 2: Between start_week and end_week
        query["Week"] = {"$gte": start_week, "$lte": end_week}

    elif start_week:
        # Filter 1: Greater than or equal to start_week
        query["Week"] = {"$gte": start_week} 

    elif end_week:
        # Filter 3: Less than or equal to end_week 
        query["Week"] = {"$lte": end_week} 


    events = list(market_bet_his_collection.find(query, {"_id": 0}))



    events = jsonable_encoder(events, custom_encoder={float: lambda x: 0 if np.isnan(x) else x})
    return events

# @app.get("/BPM_squad_his")
# def get_events_stats():
#     events = list(BPM_squad_his_collection.find({}, {"_id": 0}))
#     events = jsonable_encoder(events, custom_encoder={float: lambda x: 0 if np.isnan(x) else x})
#     return events

@app.get("/BPM_squad_his")
def get_events_stats(start_week: str = None, end_week: str = None):
    query = {}  # Start with an empty query

    if start_week and end_week:
        # Filter 2: Between start_week and end_week
        query["Week"] = {"$gte": start_week, "$lte": end_week}

    elif start_week:
        # Filter 1: Greater than or equal to start_week
        query["Week"] = {"$gte": start_week} 

    elif end_week:
        # Filter 3: Less than or equal to end_week 
        query["Week"] = {"$lte": end_week} 


    events = list(BPM_squad_his_collection.find(query, {"_id": 0}))



    events = jsonable_encoder(events, custom_encoder={float: lambda x: 0 if np.isnan(x) else x})
    return events


@app.get("/Teams_his")
def get_events_stats(start_week: str = None, end_week: str = None):
    query = {}  # Start with an empty query

    if start_week and end_week:
        # Filter 2: Between start_week and end_week
        query["Week"] = {"$gte": start_week, "$lte": end_week}

    elif start_week:
        # Filter 1: Greater than or equal to start_week
        query["Week"] = {"$gte": start_week} 

    elif end_week:
        # Filter 3: Less than or equal to end_week 
        query["Week"] = {"$lte": end_week} 


    events = list(Teams_his_collection.find(query, {"_id": 0}))



    events = jsonable_encoder(events, custom_encoder={float: lambda x: 0 if np.isnan(x) else x})
    return events


@app.get("/events-statsV2")
def get_events_stats():
    events = list(events_statsv2_collection.find({}, {"_id": 0}))
    events = jsonable_encoder(events, custom_encoder={float: lambda x: 0 if np.isnan(x) else x})
    return events


@app.post('/run_script')
def run_my_script(request: Request):
    current_directory = os.getcwd()
    os.chdir(current_directory)

    def run_script(script_name):
        """Runs a script and reports its success or failure."""
        command = f"cmd.exe /c \"python {script_name}\""
        try:
            result = subprocess.run(command, check=True, text=True, capture_output=True, shell=True)
            script = script_name.replace("src\\", "").replace(".py", "")
            print(f"{script}: {result.stdout.strip()}")
            return result.stdout.strip()  # Return stdout
        except subprocess.CalledProcessError as e:
            print(f"{script_name}: {e.stderr.strip()}")
            return e.stderr.strip()  # Return stderr

    scripts = ['final_BPM.py', 'spread_Players.py', 'prediction_BPM.py', 'Universal_Predict.py', 'Odds_Creation.py']
    output_list = []  # List to store outputs of each script
    for script in scripts:
        output = run_script(f"src\\{script}")
        output_list.append({script: output})  # Append output to list

    return JSONResponse({'output': output_list}, status_code=200)
