import  pandas as pd
from os.path import join as pjoin


def merge_gvkey_wrds(sdc_df, wrds_bridge):
    '''
    merge sdc_df with wrds_bridge
    
    '''
    name_lst = list(sdc_df.columns)
    sdc_df_comp = sdc_df
    print(sdc_df_comp.shape)
    merged_w_a = sdc_df_comp.merge(wrds_bridge, left_on='ACU', right_on = 'CUSIP', how = 'left')
    print(merged_w_a.shape)
    merged_w_t = merged_w_a.merge(wrds_bridge, left_on='TCU', right_on = 'CUSIP', how = 'left')
    print(merged_w_t.shape)
    merged_w_ua = merged_w_t.merge(wrds_bridge, left_on='AUP', right_on = 'CUSIP', how = 'left')
    print(merged_w_ua.shape)
    merged_w_ut = merged_w_ua.merge(wrds_bridge, left_on='TUP', right_on = 'CUSIP', how = 'left')
    print(merged_w_ut.shape)
    # name list update
    gvkey_name_list = []
    for key in ['ACU','TCU','AUP','TUP']:
        lst = [name+"_"+key for name in ['CUSIP', 'GVKEY', 'LINKDT', 'LINKENDDT', 'CONM'] ]
    #    print(len(lst))
        gvkey_name_list += lst
    assert len(gvkey_name_list) == 20
    
    merged_name_list = name_lst + gvkey_name_list
    merged_raw = merged_w_ut.copy()
    merged_raw.columns = merged_name_list


    return merged_raw



def merge_gvkey_evans(wrds_merged_fail, evans_bridge):
    '''
    Use after WRDS since WRDS bridge is more trustworthy
    
    '''
    evans_merged = wrds_merged_fail.merge(evans_bridge, left_on = "DEAL_NO", right_on = "DealNumber", how = "left")

    name_lst = list(wrds_merged_fail.columns)
    new_name_lst = name_lst + ['DEAL_NO_EVANS', 'AGVKEY_EVANS', 'TGVKEY_EVANS']
    evans_merged.columns = new_name_lst
    evans_merged.drop(['DEAL_NO'], axis=1)
    return evans_merged    


def wrds_gvkey_checker(df):
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
        else: # only 1 is ok
            if row.ACU_OK:
                return '-1'
            elif row.AUP_OK:
                return '-2'
            elif row.TCU_OK:
                return '-3'
            elif row.TUP_OK:
                return '-4'
            else:
                return '0'
    drop_name_lst = []
    for part in ['A', 'T']:
        for ent in ['CU', 'UP']:
            key = part+ent
            merged_raw[key+'_OK'] = merged_raw.apply(mark_gvkey_ok, key=key, axis=1)
            drop_name_lst += [key+'_OK']
            
    merged_raw['GVKEY_OVERALL'] = merged_raw.apply(mark_gvkey_total_ok, axis=1)
    
    merged_raw = merged_raw.drop(drop_name_lst, axis=1) # drop "xx_ok" variables
    
    print('Number of Conditions: \n', merged_raw['GVKEY_OVERALL'].value_counts(),'\n')
    return merged_raw



def evans_gvkey_checker(merged):
    def mark_gvkey_ok(row):
        if (row.AGVKEY_EVANS != '-1') & (pd.notna(row.AGVKEY_EVANS)) & (row.TGVKEY_EVANS != '-1') & (pd.notna(row.TGVKEY_EVANS)) & (row.GVKEY_OVERALL == '0'):
            return '1'
        elif  (row.TGVKEY_EVANS != '-1') & (pd.notna(row.TGVKEY_EVANS)) & (row.GVKEY_OVERALL == '-1'):
            return '2'
        elif (row.TGVKEY_EVANS != '-1') & (pd.notna(row.TGVKEY_EVANS)) & (row.GVKEY_OVERALL == '-2'):
            return '3'
        elif (row.AGVKEY_EVANS != '-1') & (pd.notna(row.AGVKEY_EVANS)) & (row.GVKEY_OVERALL == '-3'):
            return '4'
        elif (row.AGVKEY_EVANS != '-1') & (pd.notna(row.AGVKEY_EVANS )) & (row.GVKEY_OVERALL == '-4'):
            return '5'
        else:
            return 0
    
    merged['GVKEY_EVANS_STATUS'] = merged.apply(mark_gvkey_ok, axis = 1)
    print("Number of conditions: \n",merged['GVKEY_EVANS_STATUS'].value_counts(),'\n')
    return merged

def create_gvkey_var_evans(df, keep_status_var=False):
    '''
        df: GVKEY_EVANS_STATUS cannot be 0
        if you intend to keep GVKEY_EVANS_STATUS for further analysis (may add more interaction types), simply remove 
    '''
    if keep_status_var:
        helper_name_lst = ['CUSIP_ACU', 'GVKEY_ACU', 'LINKDT_ACU',
       'LINKENDDT_ACU', 'CONM_ACU', 'CUSIP_TCU', 'GVKEY_TCU', 'LINKDT_TCU',
       'LINKENDDT_TCU', 'CONM_TCU', 'CUSIP_AUP', 'GVKEY_AUP', 'LINKDT_AUP',
       'LINKENDDT_AUP', 'CONM_AUP', 'CUSIP_TUP', 'GVKEY_TUP', 'LINKDT_TUP',
       'LINKENDDT_TUP', 'CONM_TUP', 'GVKEY_OVERALL', 'DEAL_NO_EVANS',
       'AGVKEY_EVANS', 'TGVKEY_EVANS']
    else:
        helper_name_lst = ['CUSIP_ACU', 'GVKEY_ACU', 'LINKDT_ACU',
        'LINKENDDT_ACU', 'CONM_ACU', 'CUSIP_TCU', 'GVKEY_TCU', 'LINKDT_TCU',
        'LINKENDDT_TCU', 'CONM_TCU', 'CUSIP_AUP', 'GVKEY_AUP', 'LINKDT_AUP',
        'LINKENDDT_AUP', 'CONM_AUP', 'CUSIP_TUP', 'GVKEY_TUP', 'LINKDT_TUP',
        'LINKENDDT_TUP', 'CONM_TUP', 'GVKEY_OVERALL', 'DEAL_NO_EVANS',
        'AGVKEY_EVANS', 'TGVKEY_EVANS', 'GVKEY_EVANS_STATUS']
    
    def AGVKEY_BUILDER(row):
        '''
        row: GVKEY_EVANS_STATUS cannot be 0
        '''
        if row.GVKEY_EVANS_STATUS == '1':
            return row['AGVKEY_EVANS']
        elif row.GVKEY_EVANS_STATUS == '2':
            return row['GVKEY_ACU']
        elif row.GVKEY_EVANS_STATUS == '3':
            return row['GVKEY_AUP']
        elif row.GVKEY_EVANS_STATUS == '4':
            return row['AGVKEY_EVANS']
        elif row.GVKEY_EVANS_STATUS == '5':
            return row['AGVKEY_EVANS']

    def TGVKEY_BUILDER(row):
   
        if row.GVKEY_EVANS_STATUS == '1':
            return row['TGVKEY_EVANS']
        elif row.GVKEY_EVANS_STATUS == '2':
            return row['TGVKEY_EVANS']
        elif row.GVKEY_EVANS_STATUS == '3':
            return row['TGVKEY_EVANS']
        elif row.GVKEY_EVANS_STATUS == '4':
            return row['GVKEY_TCU']
        elif row.GVKEY_EVANS_STATUS == '5':
            return row['GVKEY_TUP']


    df['AGVKEY'] = df.apply(AGVKEY_BUILDER, axis=1)
    df['TGVKEY'] = df.apply(TGVKEY_BUILDER, axis=1)

    
    df = df.drop(helper_name_lst, axis=1)

    return df



def create_gvkey_var_wrds(df, keep_status_var=False):
    '''
    Only fit the df that successfully merge with WRDS linking table 
    '''
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



    df['AGVKEY'] = df.apply(gvkey_filter_a, axis=1)
    df['TGVKEY'] = df.apply(gvkey_filter_t, axis=1)


    if keep_status_var:
        keep_lst = [
                'ACU', 'ASIC2', 'ABL', 'ANL', 'APUBC', 'AUP', 'AUPSIC', 'AUPBL', 'AUPNAMES', 'AUPPUB',
                'BLOCK','CREEP','DA','DE','STATC','SYNOP','VAL','PCTACQ','PSOUGHTOWN','PSOUGHT','PHDA','PCTOWN','PSOUGHTT','PRIVATIZATION','DEAL_NO',
                'TCU', 'TSIC2', 'TBL', 'TNL', 'TPUBC', 'TUP', 'TUPSIC', 'TUPBL', 'TUPNAMES', 'TUPPUB','SIC_A',
                'SIC_T', 'YEAR', 
                'AGVKEY', 'TGVKEY','GVKEY_OVERALL'
            ] 
    else:
        keep_lst = [
                'ACU', 'ASIC2', 'ABL', 'ANL', 'APUBC', 'AUP', 'AUPSIC', 'AUPBL', 'AUPNAMES', 'AUPPUB',
                'BLOCK','CREEP','DA','DE','STATC','SYNOP','VAL','PCTACQ','PSOUGHTOWN','PSOUGHT','PHDA','PCTOWN','PSOUGHTT','PRIVATIZATION','DEAL_NO',
                'TCU', 'TSIC2', 'TBL', 'TNL', 'TPUBC', 'TUP', 'TUPSIC', 'TUPBL', 'TUPNAMES', 'TUPPUB','SIC_A',
                'SIC_T', 'YEAR', 
                'AGVKEY', 'TGVKEY'
            ]  
    df = df[keep_lst]
    return df
