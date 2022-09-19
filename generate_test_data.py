#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Sep 13 14:50:53 2022

This module generates realistic test data for the vote post-processing.
In a nutshell, given a snapshot of miners in the network at a current height,
(stored as miners.csv), the code:
    
    1. samples `N_SP` of these miners, together with their properties
        such as (power, balance, etc). This is based on real-world data.
    2. Assigns a vote (Approve, Reject, Abstain) to each of these miners. This
       in turn provides data for SPs and token holders (SPs can be large token holder) . 
    3. Creates a list of N_core fake core devs
    4. Assings a vote to each of these core devs. 


This vote object has the same structure as the one provided bby catalin.
In particular, this means that a vote is a dict with the following entries:
    
id                                                                  292 # id of the vote
pollId                                                                7 # id of the poll
constituentGroupId                                                    0 # groupID from the FilPoll notation
optionId                                                             19 #?
address               f3qiszzjynaka7bie2fbo2rm2ht23ae4mfg3rf52qgloon... # address from the vote
signerAddress         f3qiszzjynaka7bie2fbo2rm2ht23ae4mfg3rf52qgloon... # address from the signer
signature             {"address":"f3qiszzjynaka7bie2fbo2rm2ht23ae4mf... # signature, a dictionary
power                                                                 0 # power by that vote
balance                                               42889218555750677 # balance on that voter
lockedBalance                                                         0 # locked balance on that voter
uploaded                                                           True # has this vote been uploaded

where each signature is itself a dictionary wit the following entries:

'address':          'f3qiszzjynaka7bie2fbo2rm2ht23ae4mfg3rf52qgloonxssgu4n7yemx6cczkvfjeu3rmh3q5htupp2ntbxa',
 'pollId':          '7',
 'constituentGroupId': 0,
 'optionName':       'Approve',
 'message':          '37202d20417070726f7665',
 'signature': '02a9e7dd60e73b4eb1d48c53a39d1db36da2cafb1e0bffa73f7d09a79a9ef630f3d01807e619aa4b9ef15286aff1f440c612b9cadcc7bbf8f9237bb36ba976c90d1ffcc75e76d508a7efbf1ad7344c5c04ea2a20586cac64555efe8ca03e103ab2',
 'balance':          '42889218555750677',
 'lockedBalance':    '0',
 'power':            '0',
 'signer':           'f3qiszzjynaka7bie2fbo2rm2ht23ae4mfg3rf52qgloonxssgu4n7yemx6cczkvfjeu3rmh3q5htupp2ntbxa'


@author: JP, juan.madrigalcianci@protocol.ai
"""

from dataclasses import dataclass
from sentinel import sentinel
import numpy as np
import pandas as pd
import utils
from tqdm.auto import tqdm
import random
import string

pd.options.mode.chained_assignment = None  # default='warn'

###############################
#
###############################
# We begin by defining the signature and vote classes, with the @dataclass decorator
@dataclass
class Signature:
    address: str
    constituentGroupId: int
    optionName: str
    message: str
    balance: int
    lockedBalance: int
    power: int
    signer: str
    pollId: str = "FIP-0036 test"


@dataclass
class Vote:
    Id: str
    address: str
    signerAddress: str
    signature: dataclass
    power: int
    constituentGroupId: int
    # optionId:int
    balance: int
    lockedBalance: int
    uploaded: True
    pollId: str = "FIP-0036 test"


def generateVoteMiner(
    miner: pd.core.series.Series, Id: int, str_lng: int, vote_options: list
):
    """
    generates one arbitrary vote object from a given real miner `miner`

    Parameters
    ----------
    miner : pd.dataframe
        contains miner info.
        Has columns: ['id', 'minerId', 'height', 'pollId', 'power', 'balance',
               'lockedBalance', 'createdAt']
    Id : int
        vote id.
    str_lng : int
        length of the rnadom string to generate
    vote_options : array
        array of posssible vote outcomes.

    Returns
    -------
    vote : dataclass
        vote object containing the entries in the vote dataclass (see above).

    """
    signature = Signature(
        address=miner["ownerId"],
        constituentGroupId=1,
        optionName=np.random.choice(vote_options),
        message=get_random_string(str_lng),
        balance=miner["balance"],
        lockedBalance=miner["lockedBalance"],
        power=miner["power"],
        signer=miner["ownerId"],
    )

    vote = Vote(
        Id=str(Id).zfill(4),
        address=miner["ownerId"],
        signerAddress=miner["ownerId"],
        signature=signature,
        power=miner["power"],
        constituentGroupId=1,
        balance=miner["balance"],
        lockedBalance=miner["lockedBalance"],
        uploaded=True,
    )

    return vote


def generateCoreDevVote(Id: int, str_lng: int, vote_options: list):
    """
    generates one arbitrary vote object from a FAKE core dev

    Parameters
    ----------

    Id : int
        vote id.
    str_lng : int
        length of the rnadom string to generate
    vote_options : array
        array of posssible vote outcomes.

    Returns
    -------
    vote : dataclass
        vote object containing the entries in the vote dataclass (see above).

    """
    signature = Signature(
        address=get_random_string(str_lng),
        constituentGroupId=2,
        optionName=np.random.choice(vote_options),
        message=get_random_string(str_lng),
        balance=None,
        lockedBalance=None,
        power=None,
        signer=None,
    )

    vote = Vote(
        Id=str(Id).zfill(4),
        address=signature.address,
        signerAddress=signature.address,
        signature=signature,
        power=signature.power,
        constituentGroupId=2,
        balance=signature.balance,
        lockedBalance=signature.lockedBalance,
        uploaded=True,
    )
    return vote


def get_random_string(length: int):
    """
    generates a random string of length `length`

    Parameters
    ----------
    length : int
        length of the string.

    Returns
    -------
    result_str: str
        random string.

    """
    # choose from all lowercase letter
    letters = string.ascii_lowercase
    result_str = "".join(random.choice(letters) for i in range(length))
    return result_str


def addOwnerId(miners: pd.core.frame.DataFrame, dataBase: sentinel, height: int):
    """
    Adds owner and worker Id to a miner dataframe given its minerId

    Parameters
    ----------
    miners : pd.core.series.Series
        a snapshot contains miner info.
        Has columns: ['id', 'minerId', 'height', 'pollId', 'power', 'balance',
               'lockedBalance', 'createdAt']
    dataBase : sentinel
        a SQLachemy cursor connecting to sentinel
    height : int
        cutoff height.

    Returns
    -------
    miners : pd.core.series.Series
        same as input but with ownerId and workerId fields added

    """

    N_SP = len(miners)
    miners["ownerId"] = N_SP * [[]]
    miners["workerId"] = N_SP * [[]]
    miners["msig"] = N_SP * [[]]

    for n in tqdm(range(N_SP)):
        miner0 = miners.iloc[n]["minerId"]
        df = utils.getOwnerId(database=dataBase, miner_id=miner0, height=height)
        try:
            df = df.iloc[0]
            miners["ownerId"].iloc[n] = df["owner_id"]
            miners["workerId"].iloc[n] = df["worker_id"]
            miners["msig"].loc[n] = df["multi_addresses"]
        except:  # which happends if no owner/worker associated to that mienrID
            n += -1
    return miners


def generateVotes(N_sp: int = 100, N_core: int = 10, height: int = 2144412):
    """


    Parameters
    ----------
    N_sp : int, optional
        number of SP/token holders votes to generate The default is 100.
    N_core : int, optional
        number of core dev votes to generate. The default is 10.
    height : int, optional
        cutoff height. The default is 2144412.

    Returns
    -------
    votes_df: pandas.core.frame.DataFrame
        Pandas dataframe containing the generated votes

    """

    STR_LNG = 8
    VOTE_OPTIONS = ["Approve", "Reject", "Abstain"]
    miners = pd.read_csv("miner_info.csv")

    # Here, we compose the votes out of N_SP random SPs and N_CORE core developers
    # We begin by sampling N_SP random SPs:
    miners_who_voted = miners.sample(N_sp)
    dataBase = utils.connect_to_sentinel("SecretString.txt")
    # --------------------------------------------------------------------------------
    # we now obtain the worker and miner id from the miners that voted. This
    # requires connecting to sentinel
    print("")
    print("Obtaining worker and owner id from miner id...")
    print("")
    miners_who_voted = addOwnerId(miners_who_voted, dataBase=dataBase, height=height)

    # --------------------------------------------------------------------------------
    votes = []
    # We now generate arbitrary votes from real miners
    print("")
    print("generating arbitraty votes from real miners...")
    print("")
    for n in tqdm(range(N_sp)):
        miner = miners_who_voted.iloc[n]
        votes.append(
            generateVoteMiner(
                miner=miner, Id=n, str_lng=STR_LNG, vote_options=VOTE_OPTIONS
            )
        )
    # --------------------------------------------------------------------------------
    # We now generate arbitrary votes from fake core devs
    print("")
    print("generating arbitraty votes from FAKE core devs...")
    print("")
    for n in tqdm(range(N_core)):
        votes.append(
            generateCoreDevVote(n + N_sp, str_lng=STR_LNG, vote_options=VOTE_OPTIONS)
        )

    # --------------------------------------------------------------------------------
    # lastly, we put everything together on a pandas dataframe
    votes_df = pd.DataFrame(votes)
    return votes_df


if __name__ == "__main__":
    votes = generateVotes()
