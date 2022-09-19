#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Sep 15 10:04:44 2022




@author: juan
"""


import json
import pandas as pd
from urllib.request import urlopen
from datetime import datetime
class Votes:
    '''
    class to ping the votes
    '''
    
    def __init__(self):
        self.URL='https://api.filpoll.io/api/polls/16/view-votes'
        self.response=urlopen(self.URL)
        
    def getVotes(self):
        '''
        Retrieves an updated list of votes as a pandas dataframe on
        self.votes
        '''
        
        data_json = json.loads(self.response.read())
        df=pd.DataFrame(data_json)
        
        
        
        self.votes=df
    
    def update(self):
        '''
        prints what was the latest vote together with the number of votes casted
        '''
        self.getVotes()
        df=self.votes
        last=df['updatedAt'].max()
        print(' last vote was updated at ')
        print(last)
        print(' as of right now, there have been {} votes casted'.format(len(df)))
        
        
    
    
    
    
if __name__=='__main__':
    votes=Votes()

    votes.update()
    df=votes.votes
    
    ids=df['id']
    optionName=[]
    signer=[]
    N_votes=len(df)
    
    dictJuju={'ids':ids,
              'optionName':optionName,
              'signer':signer}
    for n in range(N_votes):
        signature=json.loads(df.iloc[n]['signature'])
        dictJuju['signer'].append(signature['signer'])
        dictJuju['optionName'].append(signature['optionName'])
    df=pd.DataFrame(dictJuju)
    df.to_csv('dataJuju.csv')
    