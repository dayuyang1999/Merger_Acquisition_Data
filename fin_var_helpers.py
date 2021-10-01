# helper func
import numpy as np
from scipy.stats.mstats import winsorize


def get_lags(sub_pd):
    sub_pd = sub_pd[['gvkey', 'year', 'sale', 'at']]
    sub_pd[['lag_year', 'lag_sale', 'lag_at']] = sub_pd[['year', 'sale', 'at']].shift(1)
    return sub_pd


def create_var(df):
    '''
    df:  financial var, must contain:
        - gvkey
        - datadate
        - and other variables you interested in
    
    '''
    pd_afr = df
    #### pre
    # create year and sort
    pd_afr['year'] = pd_afr.datadate.dt.year 
    pd_afr = pd_afr.sort_values(['gvkey', 'year', 'datadate'], ascending=True)
    
    # check, each firm-year observation should only be observed once
    pd_afr = pd_afr.groupby(['gvkey', 'year'], sort=False).tail(1)
    
    #### create 
    # keep at and sale
    ratio_pd = pd_afr[['gvkey', 'year', 'at', 'sale']].copy()
    
    # market to book ratio
    ratio_pd['m2b'] = (pd_afr['at']+pd_afr['prcc_f']*pd_afr['csho']-pd_afr['ceq']-pd_afr['txdb'])/(pd_afr['at'])
    
    # leverage
    ratio_pd['lev'] = (pd_afr['dlc']+pd_afr['dltt'])/(pd_afr['at'])
    
    # return on asset
    ratio_pd['roa'] = pd_afr['ib']/(pd_afr['at'])

    # various ratios
    ratio_pd['ppe'] = pd_afr['ppent']/(pd_afr['at'])
    ratio_pd['cash2asset'] = pd_afr['ch']/(pd_afr['at']) 
    ratio_pd['cash2sale'] = pd_afr['ch']/(pd_afr['sale'])
    ratio_pd['sale2asset'] = pd_afr['sale']/(pd_afr['at'])
    
    # current ratio
    ratio_pd['cr'] = pd_afr['act']/(pd_afr['lct']) 
    
    # sale growth
    growth_pd = pd_afr[['gvkey', 'year', 'sale', 'at']].copy()
    growth_pd[['lag_year', 'lag_sale', 'lag_at']] = growth_pd.groupby('gvkey', sort=False)[['year', 'sale', 'at']].shift(1)
    growth_pd['d_sale'] = (growth_pd['sale'] - growth_pd['lag_sale'])/growth_pd['lag_sale']
    growth_pd['d_at'] = (growth_pd['at'] - growth_pd['lag_at'])/growth_pd['lag_at']
    
    #print('check df structure ok: ', growth_pd.head(5))
    
    ratio_pd = ratio_pd.merge(growth_pd[['gvkey', 'year', 'd_sale', 'd_at']])
    
    
    print('check df created ok: \n', ratio_pd.head(1))
    
    print('\n variable lists of ratio pd: ', ratio_pd.columns)
    
    return ratio_pd
    
    
    
# def helper fun
def deal_na(df, na_thres):
    '''
    df: raw financial varibale table
        - the first 2 columns are `gvkey` and `year`
        - the rest are fianncial variables
    
    '''
    n_features = len(df.columns) - 2 # exclude gvkey and year
    print(f"totally {n_features} number of financial features, tolerance of num of missing is:", na_thres)
    ratio_pd_w = df
    # replace inf to na
    ratio_pd_w.replace([np.inf, -np.inf], np.nan, inplace=True)
    # count na of each row
    ratio_pd_w['n_na'] = ratio_pd_w.isna().sum(axis=1) # each row has how many Nas
     # only retain those Na < thres
    ratio_pd_w = ratio_pd_w[ratio_pd_w['n_na'] <= na_thres].reset_index(drop=True)
    
    for colname in ratio_pd_w.columns[2:(2+n_features)]:
        # remove outliers
        ratio_pd_w[colname] = winsorize(ratio_pd_w[colname], limits=[0.01, 0.01], nan_policy='omit')
        # impute na with mean
        ratio_pd_w[colname].fillna(value=ratio_pd_w[colname].mean(skipna=True), inplace=True)
    assert ratio_pd_w.isna().sum().sum() == 0
    return ratio_pd_w


def merge_fv_ma(df_fv_nona, df_ma):
    '''
    df_fv_nona: df_fv with no single missing value
    df_ma: 
    
    '''
    assert df_fv_nona.isna().sum().sum() == 0
    merge_a = df_ma.merge(df_fv_nona, how = 'inner', left_on=['AGVKEY', 'YEAR'], right_on = ['gvkey','year'])
    merge_a.columns = list(merge_a.columns[0:len(merge_a.columns)-15]) + [x.upper()+'_A' for x in merge_a.columns[-15:]]
    
    merge_t =  merge_a.merge(df_fv_nona, how = 'inner', left_on=['TGVKEY', 'YEAR'], right_on = ['gvkey','year'])
    merge_t.columns = list(merge_t.columns[0:len(merge_t.columns)-15]) + [x.upper()+'_T' for x in merge_t.columns[-15:]]

    #print("num of obs for original MA table: ", df_ma.shape[0], '\n')
    #print('num of obs in merged table:', merge_t.shape[0], '\n')
    
    return merge_t