#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Sep 18 23:27:17 2022

@author: juan
"""

import pandas as pd
import json
import datautils as utils

def countVote(vote,groups_of_voters,datasets,signatures):

    list_of_deals=datasets['deals']
    list_of_miners=datasets['miners']
    list_of_addresses_and_ids=datasets['addresses']
    list_of_core_devs=datasets['core']
    list_of_powers=datasets['powers']
    
    signature=json.loads(vote["signature"])
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
    #
    # Add Y's raw bytes to the SP capacity vote (Group 2)
    #
    total_power_SPs=0
    totalBytesY=0
    miners_Yi=[]
    for Y_i in SPs:
 
        power_Y_i=utils.get_power(Y_i, list_of_powers)
        #if power_Y_i.size>0:
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
    
    
    return groups_of_voters,signatures
    