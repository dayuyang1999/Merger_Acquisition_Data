import pandas as pd

def mark_if_public(row, **kwargs):
    '''
    Description: Used for analysis the public condition of participants in MA
    Use: use in the apply func for pd df
    Input: each row of sdc_df
    Output: the value of a new created variable
    '''
    if (row.APUBC == 'Public') &  (row.AUPPUB == 'Public') & (row.TPUBC == 'Public') & (row.TUPPUB == 'Public'):
        return '1'
    elif (row.APUBC == 'Public') &  (row.AUPPUB == 'Public') & (row.TPUBC == 'Public') & (row.TUPPUB != 'Public'):
        return '2'
    elif (row.APUBC == 'Public') &  (row.AUPPUB == 'Public') & (row.TPUBC != 'Public') & (row.TUPPUB == 'Public'):
        return '3'
    elif (row.APUBC == 'Public') &  (row.AUPPUB != 'Public') & (row.TPUBC == 'Public') & (row.TUPPUB == 'Public'):
        return '4'
    elif (row.APUBC == 'Public') &  (row.AUPPUB != 'Public') & (row.TPUBC == 'Public') & (row.TUPPUB != 'Public'):
        return '5'
    elif (row.APUBC == 'Public') &  (row.AUPPUB != 'Public') & (row.TPUBC != 'Public') & (row.TUPPUB == 'Public'):
        return '6'
    elif (row.APUBC != 'Public') &  (row.AUPPUB == 'Public') & (row.TPUBC == 'Public') & (row.TUPPUB == 'Public'):
        return '7'
    elif (row.APUBC != 'Public') &  (row.AUPPUB == 'Public') & (row.TPUBC == 'Public') & (row.TUPPUB != 'Public'):
        return '8'
    elif (row.APUBC != 'Public') &  (row.AUPPUB == 'Public') & (row.TPUBC != 'Public') & (row.TUPPUB == 'Public'):
        return '9'
    else:
        return '0'

def majority_filter(df):
    '''
    Description: used for filter "majority" takeover; Following “The Importance of Industry Links in Merger Waves.” The Journal of Finance 69 (2): 527–76. https://doi.org/10.1111/jofi.12122.
    Use: use for sdc_df
    input: sdc_df
    output: a new filtered sdc_df  to replace the old one
    '''
    
    cond1 = ((df.PCTACQ > 20.0) | pd.isna(df.PCTACQ))
    cond2 = cond1 & ((df.PCTOWN > 51.0) | (pd.isna(df.PCTOWN)))
    cond3 = cond2 & ((df.VAL > 1) | pd.isna(df.VAL))
    df_new = df[cond3]
    print('original df shape: ', df.shape, '\n')
    print('filtered df shape: ', df_new.shape)
    return df_new.reset_index(drop=True)


def complete_deal_filter(df):
    '''
    Desc: use for filter the complete deal out
    Use: 
    Input: sdc_df
    output: a new sdc_df
    '''

    df_new=df[df.STATC.isin(['C'])].copy()
    
    print('original df shape: ', df.shape, '\n')
    print('filtered df shape: ', df_new.shape)
    
    return df_new.reset_index(drop=True)


def self_merge_filter(df):
    '''
    Desc: 
    Input:
    Output:
    '''
    df = df[df.ACU != df.TCU]
    return df.reset_index(drop=True)


def gvkey_checker(df):
    '''
    add an indicator variable named "XXX_ok" and 'GVKEY_OVERALL' to indicate GVKEY merge conditions
    Do not downsize the df !
    '''
    merged_raw = df
    # pre define 2 inner helpers
    def mark_gvkey_ok(row, key):
    #    print(row['GVKEY_'+key])
        if pd.notna(row['GVKEY_'+key]) & (row['LINKDT_'+key] <= row['DA']) & (row['LINKENDDT_'+key] >= row['DA']):
            return 1
        else:
            return 0
        
    def mark_gvkey_total_ok(row):
        if row.ACU_OK & row.TCU_OK:
            return '1'
        elif row.AUP_OK & row.TCU_OK:
            return '2'
        elif row.ACU_OK & row.TUP_OK:
            return '3'
        elif row.AUP_OK & row.TUP_OK:
            return '4'
        else:
            return '0'
    drop_name_lst = []
    for part in ['A', 'T']:
        for ent in ['CU', 'UP']:
            key = part+ent
            merged_raw[key+'_OK'] = merged_raw.apply(mark_gvkey_ok, key=key, axis=1)
            drop_name_lst += [key+'_OK']
            
    merged_raw['GVKEY_OVERALL'] = merged_raw.apply(mark_gvkey_total_ok, axis=1)
    
    merged_raw.drop(drop_name_lst, axis=1) # drop "xx_ok" variables
    
    print('Number of Conditions: \n', merged_raw['GVKEY_OVERALL'].value_counts(),'\n')
    return merged_raw
    

def gvkey_filter(df):
    '''
    filter those condition we unable to analysis (no gvkey info for one side of ma event)
    '''

    merged_raw = df
    # filtered 0
    df_new = merged_raw[merged_raw['GVKEY_OVERALL'] != '0' ].copy()
    print('original df shape: ', df.shape, '\n')
    print('filtered df shape: ', df_new.shape)
    #print('notice that the missing value of deal')
    return df_new



def create_gvkey_var(df):
    '''
    not a filter, will not downsize_df, create new variable AGVKEY and TGVKEY
    '''
    merged_raw_no0 = df
    keep_lst = [
            'ACU', 'ASIC2', 'ABL', 'ANL', 'APUBC', 'AUP', 'AUPSIC', 'AUPBL', 'AUPNAMES', 'AUPPUB',
            'BLOCK','CREEP','DA','DE','STATC','SYNOP','VAL','PCTACQ','PSOUGHTOWN','PSOUGHT','PHDA','PCTOWN','PSOUGHTT','PRIVATIZATION','DEAL_NO',
            'TCU', 'TSIC2', 'TBL', 'TNL', 'TPUBC', 'TUP', 'TUPSIC', 'TUPBL', 'TUPNAMES', 'TUPPUB', 'YEAR'
            'AGVKEY', 'TGVKEY','GVKEY_OVERALL'
                ] 
    # inner helper
    def gvkey_filter_a(row):
        if row['GVKEY_OVERALL'] in ['1', '3']:
            return row['GVKEY_'+'ACU']
        elif row['GVKEY_OVERALL'] in ['2', '4']:
            return row['GVKEY_'+'AUP']
    
    
    
    def gvkey_filter_t(row):
        if row['GVKEY_OVERALL'] in ['1', '2']:
            return row['GVKEY_'+'TCU']
        elif row['GVKEY_OVERALL'] in ['3', '4']:
            return row['GVKEY_'+'TUP']

    # create new 
    merged_raw_no0['AGVKEY'] = merged_raw_no0.apply(gvkey_filter_a, axis=1)
    merged_raw_no0['TGVKEY'] = merged_raw_no0.apply(gvkey_filter_t, axis=1)
    merged = merged_raw_no0[keep_lst]
    return merged