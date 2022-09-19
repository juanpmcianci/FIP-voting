#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Sep 12 09:19:15 2022

@author: juan
"""
# imports required libraries
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
from sentinel import sentinel


def connect_to_sentinel(secret_string: str):
    """
    creates a connection coursor to the sentinel database

    Parameters
    ----------
    secret_string : str
        connection string to connect to sentinel. it should have the form:
        postgres://readonly:j<PASSWORD>@read.lilium.sh:13573/mainnet?sslmode=require

    Returns
    -------
    db : a sentinel object which is essenyially an sql aclhemy cuorsor connected to sentiel

    """
    f = open(secret_string, "r")
    NAME_DB = f.read()

    # initializes the class
    db = sentinel(NAME_DB)
    return db


def get_miner_sector_deals(database: sentinel, miner_id: str, height: int):
    """
    Returns a pandas dataframe with information regarding sector deals for
    miner with `miner_id` updated until `height`

    Parameters
    ----------
    database : sentinel
        db : a sentinel object which is essenyially an sql aclhemy cuorsor connected to sentiel
    miner_id : str
        id of the miner we want to get this info from
    height : int
        cutoff height.

    Returns
    -------
    sector_deals : pandas.Dataframe containing relevant data;
    "miner_id","sector_id","activation_epoch","expiration_epoch",
    "deal_weight","verified_deal_weight","height"

    """

    QUERY = """
    SELECT "miner_id","sector_id","activation_epoch","expiration_epoch",
    "deal_weight","verified_deal_weight","height"
    FROM "visor"."miner_sector_infos"            
    WHERE "height"<='{}' AND miner_id='{}'
    ORDER BY "height" DESC
   """.format(
        height, miner_id
    )
    sector_deals = database.customQuery(QUERY)
    return sector_deals


def get_owned_SPs(database: sentinel, owner_id: str, height: int):
    """
    Get SP addresses owned by or with worker id `owner_id` up until `height`

    Parameters
    ----------
    database : sentinel
        db : a sentinel object which is essenyially an sql aclhemy cuorsor connected to sentiel
    owner_id : str
        owner or worker id we want to do this with
    height : int
        cutoff height.

    Returns
    -------
    miner_infos :  pandas.Dataframe containing relevant data;
    "miner_id", "multi_addresses", "height"
    """

    QUERY = """
    SELECT DISTINCT "miner_id", "multi_addresses", "height"
    FROM "visor"."miner_infos"            
    WHERE "height"<='{}' AND owner_id='{}' OR worker_id='{}'
    ORDER BY "height" DESC
   """.format(
        height, owner_id, owner_id
    )
    miner_infos = database.customQuery(QUERY)
    return miner_infos


def get_all_power_actors(database: sentinel, max_height: int):
    """
    generates a list of all miners with their power QAP and RBP

    Parameters
    ----------
    database : sentinel
        db : a sentinel object which is essenyially an sql aclhemy cuorsor connected to sentiel

    height : int
        cutoff height.

    Returns
    -------
    power_actors : pandas dataframer containing a list of all miners with their power QAP and RBP

    """
    QUERY = """
            SELECT DISTINCT "miner_id", "height", "state_root", "raw_byte_power", "quality_adj_power"
            FROM "visor"."power_actor_claims"            
            WHERE "height"<={}
            ORDER BY "height" DESC
            """.format(
        max_height
    )
    power_actors = database.customQuery(QUERY)
    return power_actors


def get_actor_addresses(database: sentinel):
    """


    Parameters
    ----------
    database : sentinel
        db : a sentinel object which is essenyially an sql aclhemy cuorsor connected to sentiel

    Returns
    -------
    actors : pandas dataframe with list of all different actors
        DESCRIPTION.

    """

    QUERY = """SELECT DISTINCT "id" FROM "visor"."actors" """
    actors = database.customQuery(QUERY)
    return actors


def get_active_power_actors(database: sentinel, max_height: int):
    """
    generates a list of all active (with deals expiring after max_height) miners with their power QAP and RBP

    Parameters
    ----------
    database : sentinel
        db : a sentinel object which is essenyially an sql aclhemy cuorsor connected to sentiel

    height : int
        cutoff height.

    Returns
    -------
    power_actors : pandas dataframer containing a list of all active miners with their power QAP and RBP

    """
    try:
        active_powers = pd.read_csv("power_actors.csv")
    except:
        power_actors = get_all_power_actors(database=database, max_height=max_height)
        positive_powers = power_actors[power_actors["quality_adj_power"] > 0]
        active_powers = positive_powers.sort_values(
            "height", ascending=False
        ).drop_duplicates("miner_id")
        active_powers.to_csv("power_actors.csv")
    return active_powers


def get_market_deal_provider(database: sentinel, provider_id: str, height: int):
    """
    get info for all market deal proposals  for a given `provider_id`

    Parameters
    ----------
    database : sentinel
        db : a sentinel object which is essenyially an sql aclhemy cuorsor connected to sentiel
    provider_id : str
        the provider id we want to do this for
    height : int
        cutoff height

    Returns
    -------
    df : pandas dataframe containing
    "piece_cid", "deal_id",  "padded_piece_size",
    "is_verified", "client_id", "provider_id","height"

    """
    QUERY = """
        SELECT DISTINCT "piece_cid", "deal_id",  "padded_piece_size",
        "is_verified", "client_id", "provider_id","height" 
        FROM "visor"."market_deal_proposals"
        WHERE "height"<={} and "provider_id"='{}' and "end_epoch">={}
        ORDER BY "height" DESC
        """.format(
        height, provider_id, height
    )
    df = database.customQuery(QUERY)
    return df


def get_market_deal_client(database: sentinel, client_id: str, height: int):

    """
    get info for all market deal proposals  for a given `client_id`

    Parameters
    ----------
    database : sentinel
        db : a sentinel object which is essenyially an sql aclhemy cuorsor connected to sentiel
    provider_id : str
        the clientid we want to do this for
    height : int
        cutoff height

    Returns
    -------
    df : pandas dataframe containing
    "piece_cid", "deal_id",  "padded_piece_size",
    "is_verified", "client_id", "provider_id","height"

    """
    QUERY = """
            SELECT DISTINCT "piece_cid", "deal_id",  "padded_piece_size",
            "is_verified", "client_id", "provider_id","height" 
            FROM "visor"."market_deal_proposals"
            WHERE "height"<={} and "client_id"='{}' and "end_epoch">={}
            ORDER BY "height" DESC
            """.format(
        height, client_id, height
    )
    df = database.customQuery(QUERY)
    return df




if __name__ == "__main__":
    minerId = "f01740934"

    HEIGHT = 2144412

    db = connect_to_sentinel(secret_string="SecretString.txt")
    df = get_market_deal_provider(db, minerId, HEIGHT)
    owned = get_owned_SPs(db, minerId, HEIGHT)
    power_actors = get_active_power_actors(db, HEIGHT)
    # msd=get_miner_sector_deals(database=db,miner_id=minerId,height=HEIGHT)
