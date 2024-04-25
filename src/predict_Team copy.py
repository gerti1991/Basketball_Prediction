from scipy.optimize import minimize
import pandas as pd
import numpy as np
import math

# Read the uploaded CSV file into a DataFrame
df = pd.read_csv(r'C:\\Users\\tiran\Downloads\\TestTennis.csv', encoding='ISO-8859-1')

# Filter the DataFrame to include only finished matches
df_ = df[df['Status'] == 'Finished'].copy()
df_['Date'] = pd.to_datetime(df_['Date'])

# Define terrain and Epsilon
terrain = {'Hard': 1, "Clay": 0.25, "Grass": 0.5}
Epsilon = math.log(0.5) / 305
ground_map = {'Hard': 1, 'Clay': 0.25, 'Grass': 0.5}
df_['ground_coeff'] = df_['Surface'].map(ground_map)
# Create a DataFrame with initial ratings for players
players = np.union1d(df_['Winner'].unique(), df_['Loser'].unique())
df2 = pd.DataFrame({'Player': players, 'Rating': 1000.0})

# Function to calculate Ln(lk) for a given set of ratings
def calculate_ln_lk(ratings, df):
    df['Winner_Rating'] = df['Winner'].map(dict(zip(players, ratings)))
    df['Loser_Rating'] = df['Loser'].map(dict(zip(players, ratings)))
    df['P(i_j)'] = ((df['Winner_Rating'] / (df['Winner_Rating'] + df['Loser_Rating'])) ** df['Wsets']) * (
                    (df['Loser_Rating'] / (df['Winner_Rating'] + df['Loser_Rating'])) ** df['Lsets'])
    df['ln'] = np.log(df['P(i_j)'] ** df['ground_coeff'])
    df['days'] = (df['Date'] - df['Date'].min()).dt.days
    df['Epsilon'] = np.exp(Epsilon * (609 - df['days']))
    df['Ln(lk)'] = df['ln'] * df['Epsilon']
    return df['Ln(lk)'].sum()

# def calculate_ln_lk(ratings, df):
#     df['Winner_Rating'] = df['Winner'].map(dict(zip(players, ratings)))
#     df['Loser_Rating'] = df['Loser'].map(dict(zip(players, ratings)))
#     df['P(i_j)'] = df['Winner_Rating'] / (df['Winner_Rating'] + df['Loser_Rating'])
#     df['Ln(lk)'] = np.log(df['P(i_j)'])
#     return df['Ln(lk)'].sum()

# Objective function to minimize (negative sum of Ln(lk))
def objective_function(ratings, df):
    return -calculate_ln_lk(ratings, df)

# Minimize the negative sum of Ln(lk) to maximize the sum of Ln(lk)
result = minimize(objective_function, df2['Rating'].values, args=(df_,), method='SLSQP')

# Extract optimized ratings
optimized_ratings = pd.DataFrame({'Player': df2['Player'], 'Rating': result.x})

# Store the optimized ratings to df2
df2 = df2.merge(optimized_ratings, on='Player', how='left')

# Print df2 with optimized ratings
print(df2)
print(calculate_ln_lk(result.x, df_))
