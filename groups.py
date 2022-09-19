#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Sep 12 17:03:00 2022
defines classes for each of the groups

Group 1: Deal Storage (weighted by Deal Bytes)
SPs sign the voting message from their owner address or worker address
Group 2: Capacity (weighted by Raw Bytes)
SPs sign the voting message from their owner address or worker address
Group 3: Clients (weighted by Deal Bytes)
Client sign the voting message from their deal making account (the account used to propose deals)  
Group 4: Token Holders (weighted by FIL balance, for both regular wallet and multisig accounts)
Sign the voting message using their wallet account
A multisig wallet vote is valid when the number of signatures received from the token holders(that are signers of the multisig)  matches the threshold
I.e: if a multisig has 5 signers with threshold as 3, then 3 out of 5 signers must sign and submit the voting message.


@author: JP. 
juan.madrigalcianci@protocol.ai
"""
import pandas as pd
import numpy as np
class groups:
    '''
    This is a generic group object. it takes 
    
    * `groupid`, where
    
    1. deal storage
    2. capacity
    3. clients
    4. token holders
    5. core dev    
    
   * `votes` is an array counting the votes (''Aprove, "Reject", "Abstain")
   * `address` address of the person or multisig that voted
   * `quantity` displays the quantity of each vote, it could be, e.g., raw bytes, head count, etc
   * `idType` miner_id or worker_id if groupdid=1 , regular or multisig if groupid=4, else None
    
    
    '''
    def __init__(self,groupID):
        self.groupID=groupID
        self.votes=[]
        self.address=[]
        self.quantity=[]
        self.votedMoreThanOnce=[]
    
    
    def validateAndAddVote(self,signature:dict,amount=None,miner_id=None):
        '''
        validates and adds a vote (as a dict) as a new vote on this group.
        Notice that an address might be able to vote for two different groups
        but not fo the same group more than once

        Parameters
        ----------
        signature : dict
            a dictionary with the signature information


        Returns
        -------
        None.

        '''
        
        
        # gets right vote quantity 
        
        if self.groupID==1 or self.groupID==2 or self.groupID==3:
            quantity=int(signature['power'])
        elif self.groupID==4:

            quantity=int(signature['balance'])

                
        elif self.groupID==5:
            quantity=1
        else: 
            print('wrong group ID!')
        
            
        #validates the vote
        X=signature['signature']
        
        if self.is_ellegible(signature["signer"]):
        
        
            self.votes.append(signature["optionName"])
            self.address.append(signature["signer"])
            
            if miner_id is not None:
                self.address.append(miner_id)

            
            
            #adds sig
            
            
            if amount is None:
                self.quantity.append(quantity)
            else:
                self.quantity.append(amount)

        else:
            self.votedMoreThanOnce.append(X)
            
        
        
        
        
        
        
        
    def is_ellegible(self,adress:str):
        '''
        checks whether an address `adress` is ellegible (i.e., has it already voted)        

        Parameters
        ----------
        adress:: str
            address

        Returns
        -------
        is_it : boolean
            `is_it` ellegible? True or False, 

        '''
        is_it=adress not in self.address
        return is_it
    
    def getUnits(self):
        '''
        gets the units for each voting category; can be bytes, people or tokens

        Returns
        -------
        units : str
            units for the groupID

        '''
        Id=self.groupID
        if Id==1 or Id==2 or Id==3:
            units='PiB'
        elif Id==4:
            units=' FIL'
        elif Id==5:
            units=' devs'
        else:
            print('wrong group ID!')
        return units
        
    def tabulate(self):
        '''
        collects data from the group and puts it on a dataframe

        Returns
        -------
        df : pandas dataframe
            contains tabulated data of votes, addresses and stuff

        '''
        aux=[self.votes,self.address,self.quantity]
        df=pd.DataFrame(aux).T
        df.columns=['vote','address','quantity']
        self.tabulatedVotes=df
        

        
        
    def count(self):
        '''
        tallies the votes stored in the group.

        Returns
        -------
        a tally of the votes, dict

        '''
        self.tabulate()
        list_of_votes=self.tabulatedVotes
        total_votes=list_of_votes['quantity'].sum()
        print('-----------')
        
        if  self.groupID==1 or self.groupID==2 or self.groupID==3:
            divisor=2**60
        elif self.groupID==4:
            divisor=int(1e18)
        else:
            divisor=1

        self.tally = list_of_votes.groupby("vote")["quantity"].sum().to_dict()
        total_votes = sum(self.tally.values())//divisor
        for op, voted_for_op in self.tally.items():
            voted_for_op=voted_for_op//divisor
            

            
            
            print('option: '+str(op))
            print('There were a total of '+str(voted_for_op)+' '+self.getUnits()+' in favor of '+op)
            print('this represents {}% of the vote'.format(round(100*voted_for_op/(total_votes),2) ))
            print('-----------')

             

if __name__=='__main__':
    
    OUTCOMES=['Approve','Reject','Abstain']
    GROUPS=['deals','capacity','clients','tokens','devs']
    N_VOTES=100
    deals=groups(1)
    capacity=groups(2)
    clients=groups(3)
    tokens=groups(4)
    devs=groups(5)
    
    voters={
        "deals":deals,
        "capacity":capacity,
        "clients":clients,
        "tokens":tokens,
        "devs":devs}


    
    for i in range(N_VOTES):
        for gr in GROUPS:
            vote=np.random.choice(OUTCOMES)
            address='f2_sample_'+str(i)+'_'+gr
            if gr=='tokens':
                quantity=np.random.randint(int(1e18))
            elif gr=='deals' or gr=='clients' or gr=='capacity':
                quantity=np.random.randint(int(1e15),int(1e17))
            else:
                quantity=1

            voters[gr].votes.append(vote)
            voters[gr].address.append(address)
            voters[gr].quantity.append(quantity)

#Tests the tallying of the votes#
    for gr in GROUPS:
        print('###################')
        print('Counting '+str(gr))
        print('###################')
        print('')
        print('')
        
        voters[gr].count()
        
        print('')
    
        
        
        
        
        
        





        
        

        
        