import requests
import pandas as pd
import numpy as np
from pymongo import MongoClient
from helpers import Positions,Offensive_Role,Default_pos,Min_Wt,BPM_coff,Position_Constant,Role
from helpers import clean_and_convert,merge_rows_,calculate_team_average,calculate_sum_product,merge_rows
from connectors import mongo_connect,add_to_mongo

try:
    PS = mongo_connect('player_stats')
    TS = mongo_connect('team_stats')


    #Team stats
    MP_G = 10
    PS['MP_G'] = PS['MP']/PS['G']
    PS = PS[PS['MP_G'] >= MP_G]
    Coff_FTA = 0.44

    mean_offensive_rating = TS.groupby(['League', 'Season'])['Offensive Rating'].mean().reset_index()
    PS = PS.merge(mean_offensive_rating, on=['League', 'Season'], suffixes=('', '_mean'))
    PS.rename(columns={'Offensive Rating': 'Average Offensive Rating'}, inplace=True)

    # Merging 'Efficiency Differential', 'Offensive Rating', and 'Pace' from TS to PS
    PS = merge_rows_(PS, TS, Row_to_add=['Efficiency Differential', 'Offensive Rating', 'Pace'])

    # Calculating 'Team Rating' and 'Off Rating'
    PS['Team Rating'] = PS['Efficiency Differential']

    PS['Off Rating'] = PS['Offensive Rating'] - PS['Average Offensive Rating']

    # Calculating other metrics
    PS['Def Rating'] = PS['Team Rating'] - PS['Off Rating']
    PS['Avg. Lead'] = PS['Team Rating'] * PS['Pace'] / 100 / 2
    PS['Lead Bonus'] = 0.35 / 2 * PS['Avg. Lead']
    PS['Adj. Tm Rtg'] = PS['Team Rating'] + PS['Lead Bonus']
    PS['Adj. ORtg'] = PS['Off Rating'] + PS['Lead Bonus'] / 2

    team_sums = PS.groupby(['Team', 'Season', 'League']).sum()

    # Merge the aggregated data back into PS
    PS = PS.merge(team_sums[['PTS', 'FGA', 'FTA','MP', 'TRB', 'STL', 'PF', 'AST', 'BLK', 'GS']], on=['Team', 'Season', 'League'], suffixes=('', '_TeamSum'))

    # Calculations using the team sums
    PS['Tm Pts/TSA'] = PS['PTS_TeamSum'] / (PS['FGA_TeamSum'] + PS['FTA_TeamSum'] * Coff_FTA)
    PS['Total Minutes'] = PS['MP_TeamSum']
    PS['Team Games'] = PS['GS_TeamSum'] / 5
    PS['% Min'] = PS['MP'] / (PS['MP_TeamSum'] / 5)

    #Adjust for Team Shooting Context
    PS['TSA'] = PS['FGA']+Coff_FTA*PS['FTA']
    PS['Pt/TSA'] = np.where(PS['TSA'] == 0, 0, PS['PTS'] / PS['TSA'])
    PS['Adj. Pts'] = ((PS['Pt/TSA']-PS['Tm Pts/TSA'])+1)*PS['TSA']
    PS['Possessions'] = PS['MP'] * PS['Pace']/40
    PS['ThreshPts'] = PS['TSA']*(PS['Pt/TSA']-(PS['Tm Pts/TSA']+Offensive_Role["Pt Threshold"]))
    team_sums = PS.groupby(['Team', 'Season', 'League']).sum()
    PS = PS.merge(team_sums['ThreshPts'], on=['Team', 'Season', 'League'], suffixes=('', '_TeamSum'))


    # Calculated Stats per 100 Possessions
    # Step 1: Precompute Means for necessary columns
    mean_values = PS.groupby(['Team', 'Season', 'League', 'Player']).agg({'Adj. Pts': 'mean', 'FGA': 'mean', 'FTA': 'mean', '3P': 'mean', 'AST': 'mean', 'TOV': 'mean', 'ORB': 'mean', 'DRB': 'mean', 'TRB': 'mean', 'STL': 'mean', 'BLK': 'mean', 'PF': 'mean', 'Possessions': 'mean'}).reset_index()

    # Step 2: Merge the Precomputed Means
    PS_100 = PS[['League', 'Season', 'Player', 'Team', 'Position']]
    PS_100 = PS_100.drop_duplicates().merge(mean_values, on=['Team', 'Season', 'League', 'Player'])


    # Adding and calculating 'Adj Pt' per 100 Possessions
    PS_100['Adj Pt_per_100'] = PS_100['Adj. Pts']/PS['Possessions']*100

    # Step 3: Calculate Percentages including 'Adj Pt'
    for column in ['Adj. Pts', 'FGA', 'FTA', '3P', 'AST', 'TOV', 'ORB', 'DRB', 'TRB', 'STL', 'BLK', 'PF']:
        PS_100[column] = (PS_100[column] / PS_100['Possessions']) * 100



    # Renaming columns for clarity except for 'Adj Pt'
    renamed_columns = {col: col + '_per_100' for col in ['FGA', 'FTA', '3P', 'AST', 'TOV', 'ORB', 'DRB', 'TRB', 'STL', 'BLK', 'PF']}
    PS_100.rename(columns=renamed_columns, inplace=True)

    # Dropping the 'Possessions' and original 'Adj. Pts' columns as they're no longer needed
    PS_100.drop(['Possessions', 'Adj. Pts'], axis=1, inplace=True)


    #% of Stats
    # Step 1: Precompute Ratios
    grouped_PS = PS.groupby(['Team', 'Season', 'League', 'Player'])
    ratios = grouped_PS.apply(lambda df: pd.Series({
        '% of TRB': np.mean(df['TRB'] / df['TRB_TeamSum'] / df['% Min']),
        '% of STL': np.mean(df['STL'] / df['STL_TeamSum'] / df['% Min']),
        '% of PF': np.mean(df['PF'] / df['PF_TeamSum'] / df['% Min']),
        '% of AST': np.mean(df['AST'] / df['AST_TeamSum'] / df['% Min']),
        '% of BLK': np.mean(df['BLK'] / df['BLK_TeamSum'] / df['% Min']),
        '% of ThreshPts': np.mean(df['ThreshPts'] / df['ThreshPts_TeamSum'] / df['% Min'])
    })).reset_index()

    # Step 2: Merge the Ratios back into PS_100
    # PS_100 = PS[['League', 'Season', 'Player', 'Team', 'Position']]
    PS_100 = PS_100.drop_duplicates().merge(ratios, on=['Team', 'Season', 'League', 'Player'])

    PS_100['Pos Num'] = PS_100['Position'].map(Role)

    # Estimate Positions:
    # Base Position
    # Convert common columns to string type for consistency
    common_cols = ['League', 'Season', 'Player', 'Team']
    B_Position = PS_100[common_cols + ['Pos Num', '% of TRB', '% of STL', '% of PF', '% of AST', '% of BLK']].copy()
    for col in common_cols:
        B_Position[col] = B_Position[col].astype(str)

    # Vectorized calculation of 'Est Pos 1'
    keys = ['% of TRB', '% of STL', '% of PF', '% of AST', '% of BLK']
    for key in keys:
        B_Position[key] = B_Position[key] * Positions['Modern'].get(key, 0)
    B_Position['Est Pos 1'] = B_Position[keys].sum(axis=1) + Positions['Modern']['Intercept']

    # Adding 'MP_TeamSum' from PS to B_Position
    PS_selected = PS[common_cols + ['MP_TeamSum']+['MP']]
    B_Position = B_Position.merge(PS_selected, on=common_cols, how='left')

    # Vectorized calculation of 'Min Adj 1'
    min_wt = Positions['Modern']['Min Wt']
    B_Position['Min Adj 1'] = (B_Position['Est Pos 1'] * B_Position['MP'] + B_Position['Pos Num'] * min_wt) / (min_wt + B_Position['MP'])

    # Trim values between 1 and 5
    B_Position['Trim 1'] = B_Position['Min Adj 1'].clip(1, 5)

    # Define group columns for calculations
    group_cols = ['Team', 'Season', 'League']



    # Calculate 'Tm Avg 1'
    B_Position = calculate_team_average(B_Position, group_cols, 'Trim 1')

    # Calculate 'Adj Pos 2' and 'Trim 2'
    B_Position['Adj Pos 2'] = B_Position['Min Adj 1'] - (B_Position['Trim 1_Tm_Avg'] - 3)
    B_Position['Trim 2'] = B_Position['Adj Pos 2'].clip(1, 5)

    # Calculate 'Tm Avg 2'
    B_Position = calculate_team_average(B_Position, group_cols, 'Trim 2')

    # Calculate 'Adj Pos 3', 'Trim 3', and 'Tm Avg 3'
    B_Position['Adj Pos 3'] = B_Position['Min Adj 1'] - (B_Position['Trim 1_Tm_Avg'] - 3) - (B_Position['Trim 2_Tm_Avg'] - 3)
    B_Position['Trim 3'] = B_Position['Adj Pos 3'].clip(1, 5)
    B_Position = calculate_team_average(B_Position, group_cols, 'Trim 3')

    # Calculate final 'Adj Pos 4' and 'Position'
    B_Position['Adj Pos 4'] = B_Position['Min Adj 1'] - (B_Position['Trim 1_Tm_Avg'] - 3) - (B_Position['Trim 2_Tm_Avg'] - 3) - (B_Position['Trim 3_Tm_Avg'] - 3)
    B_Position['Position'] = B_Position['Adj Pos 4'].clip(1, 5)

    # Final DataFrame
    B_Position = B_Position[common_cols + ['MP_TeamSum','MP','Pos Num', 'Est Pos 1', 'Min Adj 1', 'Trim 1', 'Trim 1_Tm_Avg', 'Adj Pos 2', 'Trim 2', 'Trim 2_Tm_Avg', 'Adj Pos 3', 'Trim 3', 'Trim 3_Tm_Avg', 'Adj Pos 4', 'Position']]

    # Estimate Positions:
    # Offensive Role
    O_Position = B_Position[['League', 'Season', 'Player', 'Team', 'MP', 'MP_TeamSum']]




    # Assuming 'Offensive_Role' is a predefined dictionary with necessary keys and values

    # Vectorized calculation of 'Est Off. Role 1'
    keys = ['% of AST', '% of ThreshPts']
    PS_100_sub = PS_100[common_cols + keys]
    for key in keys:
        PS_100_sub[key] = PS_100_sub[key] * Offensive_Role.get(key, 0)
    PS_100_sub['Est Off. Role 1'] = PS_100_sub[keys].sum(axis=1) + Offensive_Role['Intercept']

    # Merge 'Est Off. Role 1' into O_Position
    O_Position = O_Position.merge(PS_100_sub[['League', 'Season', 'Player', 'Team', 'Est Off. Role 1']], on=['League', 'Season', 'Player', 'Team'], how='left')

    # Vectorized calculation of 'Min Adj 1'
    min_wt = 50
    Default_pos = 4
    O_Position['Min Adj 1'] = (O_Position['Est Off. Role 1'] * O_Position['MP'] + Default_pos * min_wt) / (min_wt + O_Position['MP'])

    # Trim values between 1 and 5
    O_Position['Trim 1'] = O_Position['Min Adj 1'].clip(1, 5)

    # Use the existing calculate_sum_product function for 'Tm Avg 1'
    O_Position = calculate_sum_product(O_Position, 'Trim 1', 'Tm Avg 1')

    # Continue with subsequent calculations
    O_Position['Adj Off. Role 2'] = O_Position['Min Adj 1'] - (O_Position['Tm Avg 1'] - 3)
    O_Position['Trim 2'] = O_Position['Adj Off. Role 2'].clip(1, 5)
    O_Position = calculate_sum_product(O_Position, 'Trim 2', 'Tm Avg 2')

    O_Position['Adj Off. Role 3'] = O_Position['Min Adj 1'] - (O_Position['Tm Avg 1'] - 3) - (O_Position['Tm Avg 2'] - 3)
    O_Position['Trim 3'] = O_Position['Adj Off. Role 3'].clip(1, 5)
    O_Position = calculate_sum_product(O_Position, 'Trim 3', 'Tm Avg 3')

    O_Position['Adj Off. Role 4'] = O_Position['Min Adj 1'] - (O_Position['Tm Avg 1'] - 3) - (O_Position['Tm Avg 2'] - 3) - (O_Position['Tm Avg 3'] - 3)
    O_Position['Offensive Role'] = O_Position['Adj Off. Role 4'].clip(1, 5)

    #Calculating BPM


    BPM = B_Position[['League', 'Season', 'Player', 'Team', 'Position']]
    BPM = merge_rows(O_Position, BPM, Row_to_add=['Offensive Role'])
    BPM = merge_rows(PS_100, BPM, Row_to_add=['Adj Pt_per_100', 'FGA_per_100', 'FTA_per_100', '3P_per_100', 'AST_per_100', 'TOV_per_100', 'ORB_per_100', 'DRB_per_100', 'TRB_per_100', 'STL_per_100', 'BLK_per_100', 'PF_per_100'])

    # Vectorized calculation for BPM coefficients
    for stat in ['Adj. Pt', 'FGA', 'FTA', '3FG', 'AST', 'TO', 'ORB', 'DRB', 'TRB', 'STL', 'BLK', 'PF']:
        pos1_coeff = BPM_coff['BPM_Coefficients']['Pos 1'][stat]
        pos5_coeff = BPM_coff['BPM_Coefficients']['Pos 5'][stat]
        if stat not in ['Adj. Pt','3FG', 'AST', 'TO', 'ORB', 'DRB', 'TRB', 'STL', 'BLK', 'PF']:
            BPM[f'{stat}_BPM'] = (5 - BPM['Offensive Role']) / 4 * pos1_coeff + (BPM['Offensive Role'] - 1) / 4 * pos5_coeff
        else:
            BPM[f'{stat}_BPM'] = (5 - BPM['Position']) / 4 * pos1_coeff + (BPM['Position'] - 1) / 4 * pos5_coeff

    # Calculating Scoring, Ballhandling, Rebounding, and Defense
    BPM['Scoring'] = BPM['Adj Pt_per_100'] * BPM['Adj. Pt_BPM'] + BPM['FGA_per_100'] * BPM['FGA_BPM'] + BPM['FTA_BPM'] * BPM['FTA_per_100'] + BPM['3FG_BPM'] * BPM['3P_per_100']
    BPM['Ballhandling'] = BPM['AST_BPM'] * BPM['AST_per_100'] + BPM['TO_BPM'] * BPM['TOV_per_100']
    BPM['Rebounding'] = BPM['ORB_BPM'] * BPM['ORB_per_100'] + BPM['DRB_BPM'] * BPM['DRB_per_100'] + BPM['TRB_BPM'] * BPM['TRB_per_100']
    BPM['Defense'] = BPM['STL_BPM'] * BPM['STL_per_100'] + BPM['BLK_BPM'] * BPM['BLK_per_100'] + BPM['PF_BPM'] * BPM['PF_per_100']

    # Position Constant Calculation
    BPM['Pos Const']= np.where(BPM['Position'] < 3,
                            (BPM['Position']-1)/2*Position_Constant['Pos 3'][0]+(3-BPM['Position'])/2*Position_Constant['Pos 1'][0],
                            (BPM['Position']-3)/2*Position_Constant['Pos 5'][0]+(5-BPM['Position'])/2*Position_Constant['Pos 3'][0])+Position_Constant['Offensive Role Slope'][0]*(BPM['Offensive Role']-3)

    BPM['Raw BPM'] = BPM['Scoring']+BPM['Ballhandling']+BPM['Rebounding']+BPM['Defense']+BPM['Pos Const']
    BPM = merge_rows(PS,BPM,Row_to_add=['Adj. Tm Rtg','% Min'])
    BPM['Contrib'] = BPM['Raw BPM']*BPM['% Min']

    team_sums = BPM.groupby(['Team', 'Season', 'League']).sum()
    BPM = BPM.merge(team_sums['Contrib'], on=['Team', 'Season', 'League'], suffixes=('', '_Sum'))
    BPM['Tm Adj.'] = (BPM['Adj. Tm Rtg']-BPM['Contrib_Sum'])/5
    BPM['BPM'] = BPM['Raw BPM'] + BPM['Tm Adj.']

    #Calculating OBPM
    OBPM = B_Position[['League','Season','Player','Team','Position']]
    OBPM = merge_rows(O_Position,OBPM,Row_to_add=['Offensive Role'])
    OBPM = merge_rows(PS_100, OBPM, Row_to_add=['Adj Pt_per_100', 'FGA_per_100', 'FTA_per_100', '3P_per_100', 'AST_per_100', 'TOV_per_100', 'ORB_per_100', 'DRB_per_100', 'TRB_per_100', 'STL_per_100', 'BLK_per_100', 'PF_per_100'])

    # Vectorized calculation for BPM coefficients
    for stat in ['Adj. Pt', 'FGA', 'FTA', '3FG', 'AST', 'TO', 'ORB', 'DRB', 'TRB', 'STL', 'BLK', 'PF']:
        pos1_coeff = BPM_coff['OBPM_Coefficients']['Pos 1'][stat]
        pos5_coeff = BPM_coff['OBPM_Coefficients']['Pos 5'][stat]
        if stat not in ['Adj. Pt','3FG', 'AST', 'TO', 'ORB', 'DRB', 'TRB', 'STL', 'BLK', 'PF']:
            OBPM[f'{stat}_OBPM'] = (5 - OBPM['Offensive Role']) / 4 * pos1_coeff + (OBPM['Offensive Role'] - 1) / 4 * pos5_coeff
        else:
            OBPM[f'{stat}_OBPM'] = (5 - OBPM['Position']) / 4 * pos1_coeff + (OBPM['Position'] - 1) / 4 * pos5_coeff

    # Calculating Scoring, Ballhandling, Rebounding, and Defense
    OBPM['Scoring'] = OBPM['Adj Pt_per_100'] * OBPM['Adj. Pt_OBPM'] + OBPM['FGA_per_100'] * OBPM['FGA_OBPM'] + OBPM['FTA_OBPM'] * OBPM['FTA_per_100'] + OBPM['3FG_OBPM'] * OBPM['3P_per_100']
    OBPM['Ballhandling'] = OBPM['AST_OBPM'] * OBPM['AST_per_100'] + OBPM['TO_OBPM'] * OBPM['TOV_per_100']
    OBPM['Rebounding'] = OBPM['ORB_OBPM'] * OBPM['ORB_per_100'] + OBPM['DRB_OBPM'] * OBPM['DRB_per_100'] + OBPM['TRB_OBPM'] * OBPM['TRB_per_100']
    OBPM['Defense'] = OBPM['STL_OBPM'] * OBPM['STL_per_100'] + OBPM['BLK_OBPM'] * OBPM['BLK_per_100'] + OBPM['PF_OBPM'] * OBPM['PF_per_100']

    # Position Constant Calculation
    OBPM['Pos Const']= np.where(OBPM['Position'] < 3,
                            (OBPM['Position']-1)/2*Position_Constant['Pos 3'][1]+(3-OBPM['Position'])/2*Position_Constant['Pos 1'][1],
                            (OBPM['Position']-3)/2*Position_Constant['Pos 5'][1]+(5-OBPM['Position'])/2*Position_Constant['Pos 3'][1])+Position_Constant['Offensive Role Slope'][1]*(OBPM['Offensive Role']-3)

    OBPM['Raw OBPM'] = OBPM['Scoring']+OBPM['Ballhandling']+OBPM['Rebounding']+OBPM['Defense']+OBPM['Pos Const']
    OBPM = merge_rows(PS,OBPM,Row_to_add=['Adj. ORtg','% Min'])
    OBPM['Contrib'] = OBPM['Raw OBPM']*OBPM['% Min']

    team_sums = OBPM.groupby(['Team', 'Season', 'League']).sum()
    OBPM = OBPM.merge(team_sums['Contrib'], on=['Team', 'Season', 'League'], suffixes=('', '_Sum'))

    OBPM['Tm Adj.'] = (OBPM['Adj. ORtg']-OBPM['Contrib_Sum'])/5
    OBPM['OBPM'] = OBPM['Raw OBPM'] + OBPM['Tm Adj.']

    # Final Stats
    FINAL_temp = B_Position[['League','Season','Player','Team','Position']]
    FINAL_temp = merge_rows(O_Position,FINAL_temp,Row_to_add=['Offensive Role'])
    FINAL_temp = merge_rows(PS,FINAL_temp,Row_to_add=['MP','% Min','G','Team Games','Pace','Team Rating','Off Rating'])
    FINAL_temp['Pos'] = FINAL_temp['Position']
    FINAL_temp['Off Role'] = FINAL_temp['Offensive Role']
    FINAL_temp['Minutes'] = FINAL_temp['MP']
    FINAL_temp['MPG'] = FINAL_temp['MP']/FINAL_temp['G']
    FINAL_temp = merge_rows(BPM,FINAL_temp,Row_to_add=['BPM'])
    # FINAL_temp['trader rating'] = np.nan  # This adds a new column with NaN values
    # FINAL_temp['trader rating'] = FINAL_temp['trader rating'].astype(float)
    FINAL_temp = merge_rows(OBPM,FINAL_temp,Row_to_add=['OBPM'])
    FINAL_temp['DBPM'] = FINAL_temp['BPM'] - FINAL_temp['OBPM']
    FINAL_temp['Contrib'] = FINAL_temp['BPM']*FINAL_temp['% Min']
    FINAL_temp['VORP'] = (FINAL_temp['BPM']+2)*FINAL_temp['% Min']*FINAL_temp['Team Games']/82
    FINAL_temp['ReMPG'] = FINAL_temp['MP']/(FINAL_temp['G']+4)
    FINAL_temp['ReMin'] = np.maximum((450 - FINAL_temp['Minutes']) / 3, 0)
    FINAL_temp['ExpBPM'] = -4.75+0.175*FINAL_temp['ReMPG']
    FINAL_temp['ReBPM'] = (FINAL_temp['Minutes']*FINAL_temp['BPM']+FINAL_temp['ExpBPM']*FINAL_temp['ReMin'])/(FINAL_temp['Minutes']+FINAL_temp['ReMin'])
    FINAL_temp['ReOBPM'] = (FINAL_temp['Minutes']*FINAL_temp['OBPM']+FINAL_temp['ExpBPM']*FINAL_temp['ReMin'])/(FINAL_temp['Minutes']+FINAL_temp['ReMin'])
    FINAL_temp['ReDBPM'] = FINAL_temp['ReBPM']-FINAL_temp['ReOBPM']
    FINAL = FINAL_temp[['League','Season','Player','Team','Position','Off Role','Minutes','MPG','BPM','OBPM','DBPM','Contrib','VORP','ReMPG','ReMin','ExpBPM','ReBPM','ReOBPM','ReDBPM','Pace','Team Rating','Off Rating']]
    rename_dict = {
        'Pace': 'Pace',
        'Team Rating': 'Rating',
        'Off Rating':'ORtg'
    }

    FINAL.rename(columns=rename_dict, inplace=True)


    #Adding to mongo DB
    final_df = FINAL
    # Convert DataFrame to dictionary
    data_dict = final_df.to_dict("records")
    add_to_mongo(data_dict,'BPM_Player')
    average_bpm_df = final_df.groupby(['League', 'Team'])['BPM'].mean().reset_index()
    average_bpm_df.rename(columns={'BPM': 'Average_BPM'}, inplace=True)


    
    av_points = 80  
    average_bpm_df['Att_MIS'] = average_bpm_df['Average_BPM'] / av_points + \
                            (-average_bpm_df['Average_BPM'] / av_points + 
                             np.sqrt((average_bpm_df['Average_BPM'] / av_points) ** 2 + 4)) / 2
    average_bpm_df['Def_MIS'] = (-average_bpm_df['Average_BPM'] / av_points + 
                             np.sqrt((average_bpm_df['Average_BPM'] / av_points) ** 2 + 4)) / 2
    


    # print(average_bpm_df)

    data_dict = average_bpm_df.to_dict("records")
    add_to_mongo(data_dict,'BPM_squad')

    print("Ok")
except Exception as e:
    print(f"Error: {e}")  # Print the error message if an exception occurs