
def merge_gvkey(sdc_df, wrds_bridge, name_lst):
    '''
    merge sdc_df with wrds_bridge
    
    '''
    sdc_df_comp = sdc_df

    merged_w_a = sdc_df_comp.merge(wrds_bridge, left_on='ACU', right_on = 'CUSIP', how = 'left')
    merged_w_t = merged_w_a.merge(wrds_bridge, left_on='TCU', right_on = 'CUSIP', how = 'left')
    merged_w_ua = merged_w_t.merge(wrds_bridge, left_on='AUP', right_on = 'CUSIP', how = 'left')
    merged_w_ut = merged_w_ua.merge(wrds_bridge, left_on='TUP', right_on = 'CUSIP', how = 'left')
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