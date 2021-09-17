# helper func

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
    
    
    
