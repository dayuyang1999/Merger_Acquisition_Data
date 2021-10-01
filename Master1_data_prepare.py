
import pandas as pd
from dataloader_helpers import concat_data, get_sic
from os.path import join as pjoin
import os


class MADataLoader():
    def __init__(self, tmp_data_path, data_path, s_year, e_year, glaspe = False, sdc_from_cache=False, pickle_sdc=True):
        '''
        data_path: where the downloaded SDC data(by year), linking table located(structure info refer to README file)
        tmp_data_path: where the new created pickle data located

        s_year : start year
        e_year : end year
        '''
        self.data_path = data_path
        self.tmp_data_path = tmp_data_path
        self.s_year = s_year
        self.e_year = e_year
        self.glaspe = glaspe
        self.pickle_sdc = pickle_sdc
        self.sdc_from_cache = sdc_from_cache

        self.glaspe_sample = 3
        self.name_lst = [
                'ACU', 'ASIC2', 'ABL', 'ANL', 'APUBC', 'AUP', 'AUPSIC', 'AUPBL', 'AUPNAMES', 'AUPPUB',
                'BLOCK','CREEP','DA','DE','STATC','SYNOP','VAL','PCTACQ','PSOUGHTOWN','PSOUGHT','PHDA','PCTOWN','PSOUGHTT','PRIVATIZATION','DEAL_NO',
                'TCU', 'TSIC2', 'TBL', 'TNL', 'TPUBC', 'TUP', 'TUPSIC', 'TUPBL', 'TUPNAMES', 'TUPPUB'    
            ]
        

        self.wrds_bridge = self.read_wrds_bridge()
        self.evans_bridge = self.read_evans_bridge()
        self.sdc_df = self.read_sdc()




    #### method for read data
    def read_wrds_bridge(self):
        glaspe = self.glaspe
        wrds_bridge =  pd.read_csv(f'{self.data_path}/wrds_bridge.csv', header=0, engine=None)
        # rename
        wrds_bridge.rename(columns={x: x.replace(' ', '_').upper().strip() for x in wrds_bridge.columns}, inplace=True)
        # only need 4 columns for MA project
        wrds_bridge = wrds_bridge[['CUSIP', 'GVKEY', 'LINKDT', 'LINKENDDT', 'CONM']]
        
        # Nas and type
        wrds_bridge[['GVKEY', 'CUSIP']] = wrds_bridge[['GVKEY', 'CUSIP']].applymap(str)
        # covvert 9 digit to 6 digit; 
        if len(wrds_bridge.CUSIP[0]) == 9:
            wrds_bridge['CUSIP'] = wrds_bridge.apply(lambda row: row['CUSIP'][:6], axis=1)
        #since there are lots of "E" in LINKENDDT(still effective presently), LINKENDDT was read as str;
        # we replace "E" as '12/31/2099' (string type), and then convert to pandas timestamp
        wrds_bridge.LINKENDDT[wrds_bridge.LINKENDDT == 'E'] = '12/31/2099'
        wrds_bridge['LINKENDDT'] = pd.to_datetime(wrds_bridge['LINKENDDT'],format = '%m/%d/%Y' )
        wrds_bridge['LINKDT'] = pd.to_datetime(wrds_bridge['LINKDT'])
        # see sample
        if glaspe:
            print("WRDS Linking Table looks like: ", wrds_bridge.sample(self.glaspe_sample))
        
        # change variable type
        

        return wrds_bridge

    def read_evans_bridge(self):
        glaspe = self.glaspe
        evans_bridge = pd.read_csv(f'{self.data_path}/evans_bridge.csv')  

        print("ATTENTION, DealNumber, tgvkey, agvkey NAs in evans_bridge are interpolated as '-1'. \n ")

        evans_bridge.DealNumber = evans_bridge.DealNumber.fillna(-1)
        evans_bridge.tgvkey = evans_bridge.tgvkey.fillna(-1)
        evans_bridge.agvkey = evans_bridge.agvkey.fillna(-1)


        evans_bridge = evans_bridge.astype('int')
        evans_bridge = evans_bridge.astype('str')
        
        if glaspe:
            print(evans_bridge.sample(self.glaspe_sample))
        
        
    
        return evans_bridge

    
    def read_sdc(self):
        pickle_it = self.pickle_sdc
        from_cache = self.sdc_from_cache
        tmp_data_path = self.tmp_data_path
        s_year = self.s_year
        e_year = self.e_year
        

        if (from_cache) & ~(os.path.isfile(pjoin(tmp_data_path , f'sdc_{s_year}_{e_year}.pickle'))):
            print(f"WARNING, Cannot load from cache; \n No compabible sdc_df cache file named 'sdc_{s_year}_{e_year}.pickle' exists in {tmp_data_path} \n")

        if (from_cache) & (os.path.isfile(pjoin(tmp_data_path , f'sdc_{s_year}_{e_year}.pickle'))):
            sdc_df = pd.read_pickle(pjoin(self.tmp_data_path , f'sdc_{s_year}_{e_year}.pickle'))
            print("loading data from previous download. Did not download again")
        else:                 
            sdc_df = concat_data(self.s_year, self.e_year, self.name_lst, self.data_path)
            sdc_df = sdc_df.reset_index()
            
            # change var type and fillna
            sdc_df = sdc_df.dropna(subset=['ACU','TCU']) # actually nothing drops
            sdc_df['DEAL_NO'] = sdc_df['DEAL_NO'].fillna(-1)
            print("ATTENTION, DEAL_NO NAs in sdc_df are interpolated as '-1'. \n ")
            sdc_df['DEAL_NO'] = sdc_df['DEAL_NO'].astype(str)
            sdc_df['TCU']  = sdc_df['TCU'].astype('str')
            sdc_df['ACU']  = sdc_df['ACU'].astype('str')
            sdc_df['TUP']  = sdc_df['TUP'].astype('str')
            sdc_df['AUP']  = sdc_df['AUP'].astype('str')


            # add sdc variable
            sdc_df = get_sic(sdc_df)
            # add year varibale
            sdc_df['YEAR'] = sdc_df.DA.dt.year
            # update name lst
        
            if pickle_it:
                print(f'saving sdc table ranging from {self.s_year} to {self.e_year} to {self.tmp_data_path}')
                sdc_df.to_pickle(pjoin(self.tmp_data_path , f'sdc_{self.s_year}_{self.e_year}.pickle'))

        if self.glaspe:
            print("SDC Data looks like:", sdc_df.sample(3).T)
        
        self.name_lst += ['SIC_A', 'SIC_T', 'YEAR'] 

        return sdc_df


    def var_type_checker(self):
        wrds_bridge = self.wrds_bridge
        evans_bridge = self.evans_bridge
        sdc_df_comp = self.sdc_df
        # gvkey, cusip match

        assert type(wrds_bridge.GVKEY[0]) == type(wrds_bridge.CUSIP[0]) == type(evans_bridge.agvkey[0]) 

        assert type(evans_bridge.agvkey[0]) == type(evans_bridge.tgvkey[0]) == type(sdc_df_comp.ACU[0]) == type(sdc_df_comp.TCU[0])
                
        # deal number match

        assert type(sdc_df_comp.DEAL_NO[0]) == type(evans_bridge.DealNumber[0])

        # time match

        assert type(sdc_df_comp.DA[0]) == type(sdc_df_comp.DE[0]) == type(wrds_bridge.LINKDT[0]) == type(wrds_bridge.LINKENDDT[0])
        
        print("variable type checking finished, No error Found. \n")










        