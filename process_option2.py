#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Sep 12 23:48:36 2022

@author: juan
"""

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Sep 12 20:48:54 2022

@author: juan
"""

import utils 
from groups import groups
import numpy as np
import pandas as pd
import json

"""

In this option we have the following flowchart:
    
    
1.Receive a signed vote from X
2. Validate the signature
3. Confirm that X exists on chain
4. (if it does) Add X's balance to the Token Holder vote (Group 4))
5. Iterate over all deals, adding up the bytes of deals where X is the proposer
    5.1 Add these bytes to the client vote (Group 3)
6. Iterate over all SPs, checking if X is the owner / worker of an SP Y
    6.1 If it is, AND the SP hasn't already voted:
        6.1.1 Add Y's raw bytes to the SP capacity vote (Group 2)
        6.1.2 Iterate over all deals, adding up the bytes of deals where Y is the provider, add these bytes to Deal Storage vote (Group 1)


where X and Y are two given addresses.



"""

print("instantiating groups..")
deals = groups(groupID=0)
capacity = groups(groupID=1)
clients = groups(groupID=2)
token_holders = groups(groupID=3)
core_devs = groups(groupID=4)
# defines a list with the groups
groups_of_voters = {
    "deals": deals,
    "clients": clients,
    "token": token_holders,
    "devs": core_devs,
}

print("reading list of votes")
list_of_votes = pd.read_csv("vote.csv")

HEIGHT = 2144412

dataBase = utils.connect_to_sentinel(secret_string="SecretString.txt")

print("querying power by actor...")

power_actors = utils.get_active_power_actors(dataBase, HEIGHT)


has_voted = []
#%%
import pdb

for ii in range(len(list_of_votes)):

    print("counting the {}th vote".format(ii))

    vote = list_of_votes.iloc[ii]
    signature = json.loads(vote["signature"])
    X = signature["address"]
    #
    # checks if it;s a core dev and adds headcount
    #
    is_core_dev = False  # utils.is_core_dev(X)
    if is_core_dev and X not in has_voted:
        groups_of_voters["core"].votes.append(signature["optionName"])
        groups_of_voters["core"].address.append(X)
        groups_of_voters["core"].quantity.append(1)
        has_voted.append(X)

    #
    # checks if exists and adds balance
    #
    exists = True  # utils.verify(X)
    if exists:
        # adds balance to Token Holders
        groups_of_voters["token"].votes.append(signature["optionName"])
        groups_of_voters["token"].address.append(X)
        groups_of_voters["token"].quantity.append(vote["balance"])
        # adds to the list of votes
        has_voted.append(X)
    #
    # Iterate over all deals, adding up the bytes of deals where X is the proposer (client)
    #

    dealsForX = utils.get_market_deal_client(
        database=dataBase, client_id=X, height=HEIGHT
    )
    totalBytes = dealsForX["padded_piece_size"].sum()

    # checks that address X has not voted and has >0 bytes as a client
    if totalBytes > 0:
        pdb.set_trace()
        # adds to the clients group entry
        groups_of_voters["clients"].votes.append(signature["optionName"])
        groups_of_voters["clients"].address.append(X)
        groups_of_voters["clients"].quantity.append(totalBytes)
        has_voted.append(X)

    #
    # Iterate over all SPs, checking if X is the owner / worker of an SP Y
    #
    Y = utils.get_owned_SPs(dataBase, X, HEIGHT)
    for i in range(len(Y)):
        Y_i = Y.iloc[i]["miner_id"]
        if Y_i not in has_voted:
            pdb.set_trace()

            #
            # Add Y's raw bytes to the SP capacity vote (Group 2)
            #
            totalBytesY = power_actors[power_actors["miner_id"] == Y_i][
                "raw_byte_power"
            ].values[0]

            signature_Y = list_of_votes[list_of_votes["signerAddress"] == Y_i]

            groups_of_voters["capacity"].votes.append(signature_Y["optionName"])
            groups_of_voters["capacity"].address.append(Y_i)
            groups_of_voters["capacity"].quantity.append(totalBytesY)
            has_voted.append(Y_i)

            #
            # Iterate over all deals, adding up the bytes of deals
            # where Y is the provider, add these bytes to Deal Storage vote (Group 1)
            #
            dealsByY = utils.get_market_deal_provider(
                database=dataBase, provider_id=Y_i, height=HEIGHT
            )

            totalBytesY = dealsByY["padded_piece_size"].sum()

            # checks that address X has not voted and has >0 bytes as a client
            if totalBytesY > 0:

                # adds to the clients group entry
                groups_of_voters["deals"].votes.append(signature["optionName"])
                groups_of_voters["deals"].address.append(Y_i)
                groups_of_voters["deals"].quantity.append(totalBytesY)
                has_voted.append(Y_i)

            has_voted.append(Y_i)
