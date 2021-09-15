import os
import numpy as np
import datetime
import pickle
from datetime import datetime, timezone
import dateutil.parser






class MADataset(torch.utils.data.Dataset):
    def __init__(self, split, data_dir = './data/tmp'):
        
        ## load data from path
        with open(os.path.join(data_dir, 'ma_modeling.pkl'), 'rb') as f:
            gvkey_ids, A_initial, A_last, ma_events = pickle.load(f)

        

        ## 
        self.N_nodes = len(unique_gvkey)
        
        self.A_initial = A_initial
        self.A_last =  A_last
        
        self.all_events =  ma_events# a list of events, see Appendix3_1 
        self.n_events = len(self.all_events)
        
        self.event_types = ['MA'] # uniform for now 
        self.assoc_types = None
        self.event_types_num = {'MA': 1} ## here is different from Dyrep

        self.time_bar = np.zeros((self.N_nodes, 1)) + self.FIRST_DATE.timestamp() # will be initalized in main.py before each epoch
    
    def get_Adjacency(self, multirelations=False):
        print('In MA, No relation type; The adjacency matrix is determited by TNIC, which update the Ad')
        return self.A_initial, self.assoc_types, self.A_last


    def __len__(self):
        return self.n_events

    def __getitem__(self, index):

        tpl = self.all_events[index]
        u, v, rel, time_cur = tpl

        time_delta_uv = np.zeros((2, 3)) # 2 nodes x (y, m, d)
        # time_bar storing the most recent previous time for all nodes        
        



        return u, v, time_delta_uv, k, time_bar, time_cur



        
        
            

