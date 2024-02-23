import pandas as pd
from pymongo import MongoClient
import requests
from helpers import clean_and_convert,merge_rows_,calculate_team_average,calculate_sum_product,merge_rows
from connectors import mongo_connect,add_to_mongo
import numpy as np


try:
    PS = mongo_connect('player_stats')
    FINAL = mongo_connect('BPM_Player')



    FINAL_temp_2 = PS[['League','Season','Player','Team','Position','G','MP','FTA','ORB','TOV','FGA']]
    FINAL_temp_2 = merge_rows(FINAL,FINAL_temp_2,Row_to_add=['BPM','trader rating','Missing'])
    FINAL_temp_2 = FINAL_temp_2[FINAL_temp_2['BPM'].notna()]
    FINAL_temp_2['% MIN'] = FINAL_temp_2['MP']/(FINAL_temp_2['G']*200)
    FINAL_temp_2['Estimated Possessions'] = FINAL_temp_2.apply(lambda row: max(0, -2.23 + (1.07 * row['FGA']) + (0.37 * row['FTA']) + (-1.23 * row['ORB']) + (0.87 * row['TOV'])), axis=1)
    FINAL_temp_2['Possessions/Min'] = FINAL_temp_2['Estimated Possessions']/FINAL_temp_2['MP']
    FINAL_temp_2['BPM'] = np.where(
    (pd.isna(FINAL_temp_2['trader rating']) | (FINAL_temp_2['trader rating'] == '') | (FINAL_temp_2['trader rating'].isnull())), 
    FINAL_temp_2['BPM'],  # Keep 'BPM' value if 'trader rating' is NaN or empty
    FINAL_temp_2['trader rating']  # Use 'trader rating' value otherwise
)
    FINAL_2 = FINAL_temp_2[['League','Season','Team','Player','Position','% MIN','Estimated Possessions','Possessions/Min','BPM','Missing']]



    #Adding to mongo DB
    final_df = FINAL_2

    # Convert DataFrame to dictionary
    data_dict = final_df.to_dict("records")
    add_to_mongo(data_dict,'BPM_Player_Spread')
    print("Ok")

except Exception as e:
    print(f"Error: {e}")  # Print the error message if an exception occurs