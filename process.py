#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Sep 15 09:08:09 2022

@author: juan
"""

import datautils as utils 
from groups import groups
import pandas as pd
import json
from votes import Votes
from tqdm import tqdm
from preprocess import dataPreprocess
#%%

HEIGHT = 2162760
datasets=dataPreprocess(height=HEIGHT,sectreString='SecretString.txt')

list_of_deals=datasets['deals']
list_of_votes=datasets['votes']
list_of_miners=datasets['miners']
list_of_addresses_and_ids=datasets['addresses']
list_of_core_devs=datasets['core']
list_of_powers=datasets['powers']

list_of_addresses=list(list_of_addresses_and_ids['address'])

N_votes=len(list_of_votes)
#%%

signatures=[]

deal=groups(1)
capacity=groups(2)
client=groups(3)
token=groups(4)
core=groups(5)


groups_of_voters={
    'core':core,
    'token':token,
    'client':client,
    'capacity':capacity,
    'deal':deal
    }

print('')
print('begin counting...')
print('')



#%%
for ii in tqdm(range(N_votes)):


    vote = list_of_votes.iloc[ii]
    signature = json.loads(vote["signature"])
    
    
    
    X = signature["address"]
    

    
    
    signature=utils.addShortAndLongId(signature=signature,
                list_of_addresses_and_ids=list_of_addresses_and_ids)
    

    #
    # checks if it;s a core dev and adds headcount
    #
    is_core_dev =utils.is_in_list(X, list_of_core_devs)
    


    if is_core_dev:
        groups_of_voters["core"].validateAndAddVote(signature)

    #
    # checks if exists and adds balance
    #
    #exists = utils.is_in_list(X, list_of_addresses)
    #if exists:
    groups_of_voters["token"].validateAndAddVote(signature)
    #
    # Iterate over all deals, adding up the bytes of deals where X is the proposer (client)
    #
    
    
    
    

    
    dealsForX=utils.get_market_deals_from_id(list_of_deals,
                                             user_id=signature['short'],
                                             side='client_id')


    
    #print('length {}'.format(len(dealsForX)))
    totalBytes = dealsForX["padded_piece_size"].sum()
    
    
    
    # checks that address X has not voted and has >0 bytes as a client
    if totalBytes > 0:
        groups_of_voters["client"].validateAndAddVote(signature,amount=totalBytes)

    #
    # Iterate over all SPs, checking if X is the owner / worker of an SP Y
    #
    
    
    SPs= utils.get_owners_and_workers(list_of_miners, signature['short'])
    
    N_sp=SPs.shape[0]
    
    

    
    #
    # Add Y's raw bytes to the SP capacity vote (Group 2)
    #
    total_power_SPs=0
    totalBytesY=0
    miners_Yi=[]
    for Y_i in SPs:
        
     
        
        
        power_Y_i=utils.get_power(Y_i, list_of_powers)
        if power_Y_i.size>0:
            total_power_SPs+=power_Y_i
    

        
        #
        # Iterate over all deals, adding up the bytes of deals
        # where Y is the provider, add these bytes to Deal Storage vote (Group 1)
        #
        
        
        dealsByY=utils.get_market_deals_from_id(list_of_deals,
                                                 user_id=Y_i,
                                                 side='provider_id')
            
            
        
        

        totalBytesY += dealsByY["padded_piece_size"].sum()
    #
    # from Add Y's raw bytes to the SP capacity vote (Group 2)
    #
        miners_Yi.append(Y_i)
    
    groups_of_voters["capacity"].validateAndAddVote(signature,amount=total_power_SPs,miner_id=miners_Yi)
    groups_of_voters["deal"].validateAndAddVote(signature,amount=totalBytesY)
    

    
    signatures.append(signature)




#%%
GROUPS=['deal','capacity','client','token','core']

for gr in GROUPS:
    print('###################')
    print('Counting '+str(gr))
    print('###################')
    print('')
    print('')
    try:
        groups_of_voters[gr].count()
    except:
        pass
    
    print('')
