from pymongo import MongoClient

# Connect to MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client['Basketball']  # Replace 'yourDatabaseName' with your actual database name
collection = db['events_stats']  # Replace 'yourCollectionName' with your actual collection name

# Update operation to unset fields
collection.update_many(
    {},  # Filter for the documents; an empty dictionary {} matches all documents in the collection.
    {
        '$unset': {
            'Att_MIS_x': '',  # The value is ignored, just need to specify the field name
            'Att_MIS_y': '',
            'Def_MIS_x': '',
            'Def_MIS_y': ''
        }
    }
)