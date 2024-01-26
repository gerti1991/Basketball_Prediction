from fastapi import FastAPI
from pymongo import MongoClient
from fastapi.encoders import jsonable_encoder
import json
import numpy as np

app = FastAPI()

# Connect to MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client.Basketball  # Database name
events_stats_collection = db.events_stats  # Collection name
market_bet_collection = db.market_bet


@app.get("/")
def read_root():
    return {"Hello": "World"}


@app.get("/events-stats")
def get_events_stats():
    events = list(events_stats_collection.find({}, {"_id": 0}))  # Fetch data, excluding the _id field
    events = jsonable_encoder(events, custom_encoder={float: lambda x: 0 if np.isnan(x) else x})
    return events

events_stats_collection2 = db.BPM_Player
@app.get("/BPM_Player")
def get_events_stats():
    events = list(events_stats_collection2.find({}, {"_id": 0}))  # Fetch data, excluding the _id field
    events = jsonable_encoder(events, custom_encoder={float: lambda x: 0 if np.isnan(x) else x})
    return events

@app.get("/market_bet")
def get_events_stats():
    events = list(market_bet_collection.find({}, {"_id": 0}))  # Fetch data, excluding the _id field
    events = jsonable_encoder(events, custom_encoder={float: lambda x: 0 if np.isnan(x) else x})
    return events

