import pandas as pd
import numpy as np
from connectors import mongo_connect,add_to_mongo
from xgboost import XGBRegressor
from sklearn.model_selection import train_test_split

events= mongo_connect('events_stats')

events['BPM'] = events['Prediction_Home_BPM']*-1
events['Market'] = events['Prediction_Home_Market']*-1
events['Final'] = events['Home Score']-events['Away Score']
df2 = events[events['Status']=='Finished'][['BPM','Market','Final']]
df2

X = df2[['BPM', 'Market']]  # Features
y = df2['Final']            # Target

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# Create the XGBoost model
xgb_model = XGBRegressor(n_estimators=100, learning_rate=0.1, random_state=42)

# Fit the model

xgb_model.fit(X_train, y_train)

xgb_predictions = xgb_model.predict(X_test)

# Assuming X_test is your test dataset
df3 = events[events['Status']=='Scheduled']
df3['BPM'] = df3['Prediction_Home_BPM']*-1
df3['Market'] = df3['Prediction_Home_Market']*-1
X_new = df3[['BPM','Market']]
xgb_predictions = xgb_model.predict(X_new)
df3['ML_Predict'] = xgb_predictions
df3 = df3[['Event ID', 'ML_Predict']]
events = events.merge(df3,on="Event ID", how='left')
events = events.fillna(0)

# events_stats = events
# data_dict_events_stats = events_stats.to_dict("records")
# add_to_mongo(data_dict_events_stats,'events_stats')
# print(events[events['Status']=='Scheduled'])
print(df3)

