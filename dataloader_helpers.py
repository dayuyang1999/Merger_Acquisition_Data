# define a helper function

import pandas as pd
import datetime 
import numpy as np




def concat_data(st, end, name_lst, data_path):
    df_l = []
    for year in range(st, end+1, 1):
        df = pd.read_excel(f"{data_path}/SDC/{year}.xlsx", header=1, engine='openpyxl')
        #df = df.drop(df.columns[4], axis=1) # this column is duplicate with column 3 
        #print(len(df.columns))
        df.columns = name_lst
        
        # check date var loading ok
        check = df[df['DA'] == datetime.time(0, 0)]
        if check.shape[0] == 0 :
            print('date variables loading ok \n')
        else:
            print('date variables loading fail, please manually check. number of failed records: ', check.shape[0])
        
        df_l.append(df)
        print(f'{year} data shape:', df.shape)
        del df
    df = pd.concat(df_l)
    return df



def get_sic(df):
    '''
    df: the sdc table contains sic variable named as `ASIC2`
    
    '''
    x = df.ASIC2.str.split('/')
    x = x.transform(lambda x: x[0] if not isinstance(x, float) else np.nan)
    df['SIC_A'] = x

    x = df.ASIC2.str.split('/')
    x = x.transform(lambda x: x[0] if not isinstance(x, float) else np.nan)
    df['SIC_T'] = x
    
    return df 

def plot_missing(df):
    from matplotlib import pyplot as plt
    
    plt.figure(figsize=(12,5))
    plt.plot(list(df.isnull().sum(axis=1)), label = "num of missing") 
    plt.grid()
    plt.title('row missing')
    plt.legend()
    plt.show()
    
    plt.figure(figsize=(12,5))
    axes = plt.gca()
    axes.set_ylim([0, df.shape[0]])
    plt.plot(list(df.isnull().sum(axis=0)),  label = "num of missing")
    xint = range(0, df.shape[1])
    plt.xticks(xint)
    plt.title('colomn missing')
    plt.grid()
    plt.legend()
    plt.show()
    
    print("variable missing percentage : \n", (df.isnull().sum(axis=0) / df.shape[0])*100)
    print("variable names: ", df.columns)


def build_query(dataset, **kwargs):
    """Build Query for WRDS database.
        Args:
            dataset (str): dataset
            start (str): Start of time period with format YYYY-MM-DD, default to None
            end (str): End of time period with format YYYY-MM-DD, default to None
            columns (str or list): Filter of one or a list of columns, default to None
            sic (str or list): Filter of one or a list of SIC, default to None
            sich (str or list): Filter of one or a list of historical SIC, default to None
            gvkey (str or list):  Filter of one or a list of Global Company Key, default to None
            tic (str or list):  Filter of one or a list of ticker symbols, default to None
            cusip (str or list):  Filter of one or a list of CUSIP symbols, default to None
            limit (int): Return only a number of return records, defualt to None
        Returns:
            (str) with query
    """
    columns = kwargs.get("columns", None)
    sic = kwargs.get("sic", None)
    sich = kwargs.get("sich", None)
    start = kwargs.get("start", None)
    end = kwargs.get("end", None)
    gvkey = kwargs.get("gvkey", None)
    tic = kwargs.get("tic", None)
    cusip = kwargs.get("cusip", None)
    limit = kwargs.get("limit", None)


    if columns is not None:
        columns_filter = ",".join([x for x in columns])
    else:
        columns_filter = "*"

    date_filter = ""
    if start and end is not None:
        date_filter = "datadate between {} and {}".format("'" + start + "'" , "'" + end + "'")

    sic_filter = ""
    if sic is not None:
        sic_filter = "sic in ({}) ".format(
            ",".join(["'" + x + "'" for x in sic])
        )

    sich_filter = ""
    if sich is not None:
        sich_filter = "sich in ({}) ".format(
            ",".join(["'" + x + "'" for x in sich])
        )

    gvkey_filter = ""
    if gvkey is not None:
        gvkey_filter = "gvkey in ({}) ".format(
            ",".join(["'" + x + "'" for x in gvkey])
        )

    tic_filter = ""
    if tic is not None:
        tic_filter = "tic in ({}) ".format(
            ",".join(["'" + x + "'" for x in tic])
        )

    cusip_filter = ""
    if cusip is not None:
        cusip_filter = "cusip in ({}) ".format(
            ",".join(["'" + x + "'" for x in cusip])
        )

    # FIRST INSTANCE WITHOUT AND THEN SEQUENTIALLY ADD FILTERS (if non empty)
    filters = [date_filter, cusip_filter, tic_filter, gvkey_filter, sic_filter]
    filters_list = ""
    if any(filters):
        filters_list = " and ".join([x for x in filters if x != ""])
        filters_list = "where " + filters_list

    limit_number = ""
    if limit:
        limit_number += "LIMIT {} ".format(limit)

    cmd = (
        "select "
        + columns_filter
        + " from {} ".format(dataset)
        + filters_list
        + limit_number
        )

    print(cmd)

    return(cmd)





def get_firm_annual_data(tmp_data_path, s_year, e_year):
    # Among the selected variables, for those money denominated variables, the unit is million.
    pd_afr = db.raw_sql(sql=f'''
      select gvkey, datadate, at, ceq, csho, prcc_f, txdb, dlc, dltt, ib, sale, ch, ppent, re, act, lct
      from comp.funda
      where extract(year from datadate) >= {s_year} AND extract(year from datadate) <= {e_year}
    ''', date_cols=['datadate'])

    pd_afr.gvkey = pd_afr.gvkey.astype(int).astype(str) # ! keep in mind that we do not allow 00 at front
    pd_afr['year'] = pd.DatetimeIndex(pd_afr['datadate']).year
    pd_afr.to_pickle(f"./{tmp_data_path}/fin_raw_{s_year}_{e_year}.pickle")

