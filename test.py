OBPM['Scoring'] = OBPM['Adj Pt_per_100'] * OBPM['Adj. Pt_OBPM'] + OBPM['FGA_per_100'] * OBPM['FGA_OBPM'] + OBPM['FTA_OBPM'] * OBPM['FTA_per_100'] + OBPM['3FG_OBPM'] * OBPM['3P_per_100']
OBPM['Ballhandling'] = OBPM['AST_OBPM'] * OBPM['AST_per_100'] + OBPM['TO_OBPM'] * OBPM['TOV_per_100']
OBPM['Rebounding'] = OBPM['ORB_OBPM'] * OBPM['ORB_per_100'] + OBPM['DRB_OBPM'] * OBPM['DRB_per_100'] + OBPM['TRB_OBPM'] * OBPM['TRB_per_100']
OBPM['Defense'] = OBPM['STL_OBPM'] * OBPM['STL_per_100'] + OBPM['BLK_OBPM'] * OBPM['BLK_per_100'] + OBPM['PF_OBPM'] * OBPM['PF_per_100']

# Position Constant Calculation
OBPM['Pos Const']= np.where(OBPM['Position'] < 3,
                           (OBPM['Position']-1)/2*Position_Constant['Pos 3'][0]+(3-OBPM['Position'])/2*Position_Constant['Pos 1'][0],
                           (OBPM['Position']-3)/2*Position_Constant['Pos 5'][0]+(5-OBPM['Position'])/2*Position_Constant['Pos 3'][0])+Position_Constant['Offensive Role Slope'][0]*(OBPM['Offensive Role']-3)

OBPM['Raw OBPM'] = OBPM['Scoring']+OBPM['Ballhandling']+OBPM['Rebounding']+OBPM['Defense']+OBPM['Pos Const']
OBPM = merge_rows(PS,OBPM,Row_to_add=['Adj. Tm Rtg','% Min'])
OBPM['Contrib'] = OBPM['Raw OBPM']*OBPM['% Min']

team_sums = OBPM.groupby(['Team', 'Season', 'League']).sum()
OBPM = OBPM.merge(team_sums['Contrib'], on=['Team', 'Season', 'League'], suffixes=('', '_Sum'))

OBPM['Tm Adj.'] = (OBPM['Adj. Tm Rtg']-OBPM['Contrib_Sum'])/5
OBPM['OBPM'] = OBPM['Raw OBPM'] + OBPM['Tm Adj.']
