import requests
import pandas as pd
import numpy as np
from pymongo import MongoClient

# Variables needed for model
Positions = {
        "Modern": {
            "Intercept": 2.130,
            "% of TRB": 8.668,
            "% of STL": -2.486,
            "% of PF": 0.992,
            "% of AST": -3.536,
            "% of BLK": 1.667,
            "Min Wt": 50
        },
        "Pre 1971": {
            "Intercept": 1.590,
            "% of TRB": 10.892,
            "% of STL": None,
            "% of PF": 1.468,
            "% of AST": -4.400,
            "% of BLK": None,
            "Min Wt": None
        }
    }

Offensive_Role = {
    "Intercept": 6.000,
    "% of AST": -6.642,
    "% of ThreshPts": -8.544,
    "Pt Threshold": -0.330
}

Default_pos = 4
Min_Wt = 50

BPM_coff = {
    "BPM_Coefficients": {
        "Pos 1": {
            "Adj. Pt": 0.860,
            "FGA": -0.560,
            "FTA": -0.246,
            "3FG": 0.389,
            "AST": 0.580,
            "TO": -0.964,
            "ORB": 0.613,
            "DRB": 0.116,
            "TRB": 0.000,
            "STL": 1.369,
            "BLK": 1.327,
            "PF": -0.367
        },
        "Pos 5": {
            "Adj. Pt": 0.860,
            "FGA": -0.780,
            "FTA": -0.343,
            "3FG": 0.389,
            "AST": 1.034,
            "TO": -0.964,
            "ORB": 0.181,
            "DRB": 0.181,
            "TRB": 0.000,
            "STL": 1.008,
            "BLK": 0.703,
            "PF": -0.367
        }
    },
    "OBPM_Coefficients": {
        "Pos 1": {
            "Adj. Pt": 0.605,
            "FGA": -0.330,
            "FTA": -0.145,
            "3FG": 0.477,
            "AST": 0.476,
            "TO": -0.579,
            "ORB": 0.606,
            "DRB": -0.112,
            "TRB": 0.000,
            "STL": 0.177,
            "BLK": 0.725,
            "PF": -0.439
        },
        "Pos 5": {
            "Adj. Pt": 0.605,
            "FGA": -0.472,
            "FTA": -0.208,
            "3FG": 0.477,
            "AST": 0.476,
            "TO": -0.882,
            "ORB": 0.422,
            "DRB": 0.103,
            "TRB": 0.000,
            "STL": 0.294,
            "BLK": 0.097,
            "PF": -0.439
        }
    }
}

Position_Constant = {
    "Pos 1": [-0.818, -1.698],
    "Pos 3": [0, 0],
    "Pos 5": [0, 0],
    "Offensive Role Slope": [1.387, 0.43]
}

Role = {
    "PG": 1,
    "SG": 2,
    "SF": 3,
    "PF": 4,
    "C": 5,
    "G-F": 2.5,
    "GF": 2.5,
    "F-G": 2.5,
    "G": 1.5,
    "F": 3.5,
    "?": 3,
    "nan": 3

}

def clean_and_convert(df, non_numeric_columns):
    # Convert non-numeric columns to string
    for col in non_numeric_columns:
        df[col] = df[col].astype(str)

    # Determine which columns should be converted to floats
    columns_to_convert = [col for col in df.columns if col not in non_numeric_columns]

    # Convert other columns to floats, handling non-numeric data
    for column in columns_to_convert:
        # Replace non-numeric characters if necessary
        df[column] = df[column].replace('[^0-9.-]', '', regex=True)
        # Convert column to numeric, coerce errors to NaN
        df[column] = pd.to_numeric(df[column], errors='coerce')


def merge_rows_(PS, TS, Row_to_add):
    common_cols = ['League', 'Season', 'Team']

    # Convert the common columns to string type for both DataFrames
    for col in common_cols:
        PS[col] = PS[col].astype(str)
        TS[col] = TS[col].astype(str)

    # Exclude columns from Row_to_add that already exist in PS
    Row_to_add = [col for col in Row_to_add if col not in PS.columns]

    # Select only the necessary columns from TS for merging
    TS_selected = TS[common_cols + Row_to_add]

    # Merge the DataFrames on the common columns
    merged_df = PS.merge(TS_selected, on=common_cols, how='left')

    return merged_df

# Function to calculate team average
def calculate_team_average(df, group_cols, calc_col):
    sum_product = df.groupby(group_cols).apply(lambda g: (g['MP'] * g[calc_col]).sum())
    df = df.merge(sum_product.rename(calc_col + '_Tm_Avg'), on=group_cols, how='left')
    df[calc_col + '_Tm_Avg'] = df[calc_col + '_Tm_Avg'] / df['MP_TeamSum']
    return df

def calculate_sum_product(df, calc_col, result_col_name):
  group_cols = ['Team', 'Season', 'League']

  # Calculate the sum product within each group
  sum_product_series = df.groupby(group_cols).apply(lambda g: (g['MP'] * g[calc_col]).sum())

  # Merge the sum product results back into the original DataFrame
  df = df.merge(sum_product_series.rename(result_col_name), how='left', on=group_cols)
  df[result_col_name] = df[result_col_name]/df['MP_TeamSum']
  return df

def merge_rows(PS, df,Row_to_add=['MP', 'Total Minutes']):
  common_cols = ['League', 'Season', 'Player', 'Team']
  for col in common_cols:
      df[col] = df[col].astype(str)
      PS[col] = PS[col].astype(str)

  # Select only the necessary columns from PS for merging
  PS_selected = PS[common_cols + Row_to_add]

  # Merge the DataFrames on the common columns
  return df.merge(PS_selected, on=common_cols, how='left')