#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Jan 12 19:55:27 2024

@author: johnomole
"""

import logging
import os
import sqlite3
import sys
import string
import json
punctuations = string.punctuation

logging.basicConfig(format="%(asctime)s %(name)s %(levelname)-10s %(message)s")
LOG = logging.getLogger("Ingestion Pipeline")
LOG.setLevel(os.environ.get("LOG_LEVEL", logging.DEBUG))


"""
This script should accept a date as a parameter and should produce a json messages.json)
with all processed messages (all fields) with a date greater or equal to the one specified as a parameter. 
"""

def create_db_connection(db_name:str) ->tuple:
    """

    Parameters
    ----------
    db_name : TYPE
        DESCRIPTION.

    Returns
    -------
    connection : TYPE
        DESCRIPTION.
    cursor : TYPE
        DESCRIPTION.

    """
    connection = sqlite3.connect(db_name)
    cursor = connection.cursor()
    return connection, cursor


def commit_connection(connection, cursor, close_cobbection=True):
    """

    Parameters
    ----------
    connection : TYPE
        DESCRIPTION.
    cursor : TYPE
        DESCRIPTION.
    close_cobbection : TYPE, optional
        DESCRIPTION. The default is True.

    Returns
    -------
    None.

    """
    try:
        connection.commit()
        if close_cobbection:
            cursor.close()
            connection.close()
    except Exception as e:
        LOG.info(f"connection problem: {e}")

def query_db(db_name:str, query: str, date: str) -> list:
    """

    Parameters
    ----------
    db_name : str
        DESCRIPTION.
    query : str
        DESCRIPTION.
    date : str
        DESCRIPTION.

    Returns
    -------
    list
        DESCRIPTION.

    """
    connection, cursor = create_db_connection(db_name)
    cursor.execute(query, (date, ))
    data = cursor.fetchall()
    commit_connection(connection, cursor)
    return data

def generate_json(data:list) -> None:
    """

    Parameters
    ----------
    data : list
        DESCRIPTION.

    Returns
    -------
    None
        DESCRIPTION.

    """
    review_data = []
    for row in data:
        # row = data[0]
        uuid = row[0]
        timestamp = row[1]
        message = row[2]
        category = row[3]
        num_lemm = row[4]
        num_char = row[5]

        review_data.append({
                "uuid": uuid,
                "timestamp": timestamp,
                "message": message,
                "category": category,
                "num_lemm": num_lemm,
                "num_char": num_char
            })

    review_json_data = json.dumps({"num": len(data), "message": review_data})
    save_to_json(review_json_data, 'messages.json')
    
def save_to_json(data:dict, file_path:str) -> None:
    with open(file_path, 'w') as json_file:
        json_file.write(data)

if __name__=="__main__":
    date =sys.argv[1]
    db_name = "sup-san-reviews.db"
    query = """
                select
                    *
                from proc_messages
                WHERE timestamp >= ?
    """

    data = query_db(db_name, query, date)

    generate_json(data)