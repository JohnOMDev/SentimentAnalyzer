#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Jan 12 19:55:01 2024

@author: johnomole
"""

import logging
import os
import sqlite3
import string
import spacy
punctuations = string.punctuation

logging.basicConfig(format="%(asctime)s %(name)s %(levelname)-10s %(message)s")
LOG = logging.getLogger("Ingestion Pipeline")
LOG.setLevel(os.environ.get("LOG_LEVEL", logging.DEBUG))

nlp = spacy.load("en_core_web_sm")
stop_words = spacy.lang.en.stop_words.STOP_WORDS

"""
This script is responsible of moving the data from table raw_messages to proc_messages.
This table has the same fields as the previous one plus the following ones: 
    category, num_lemm (number of lemmas) and num_char (number of characters)
"""
def create_db_connection(db_name:str) -> tuple:
    """

    Parameters
    ----------
    db_name : str
        DESCRIPTION.

    Returns
    -------
    tuple
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


def query_db(query:str) -> list:
    """

    Parameters
    ----------
    query : str
        DESCRIPTION.

    Returns
    -------
    list
        DESCRIPTION.

    """
    connection, cursor = create_db_connection(db_name)
    cursor.execute(query)
    data = cursor.fetchall()
    commit_connection(connection, cursor)
    return data


def create_table(db_name:str, create_table_query:list) -> None:
    """

    Parameters
    ----------
    db_name : str
        DESCRIPTION.
    create_table_query : list
        DESCRIPTION.

    Returns
    -------
    None
        DESCRIPTION.

    """
    try:
        
        if len(create_table_query) > 1:
            connection, cursor = create_db_connection(db_name)
            for query in create_table_query:
                cursor.execute(query)
        commit_connection(connection, cursor)
    except Exception as e:
        LOG.info(f"db creation error: {e}")


def dynamic_insert(db_name:str, row: tuple) -> None:
    """

    Parameters
    ----------
    db_name : str
        DESCRIPTION.
    row : tuple
        DESCRIPTION.

    Returns
    -------
    None
        DESCRIPTION.

    """
    try:
        insert_query = """
                    INSERT INTO proc_messages (uuid, timestamp, 
                                                    message, category,
                                                    num_lemm, num_char)
                    VALUES (?, ?, ?, ?, ?, ?)
                        """
        connection, cursor = create_db_connection(db_name)
        cursor.execute(insert_query, row)
        commit_connection(connection, cursor)
    except Exception as e:
        LOG.info(f"inserting into table error: {e}")


def control_log_table(db_name:str, query:str) -> None:
    """

    Parameters
    ----------
    db_name : str
        DESCRIPTION.
    query : str
        DESCRIPTION.

    Returns
    -------
    None
        DESCRIPTION.

    """
    try:
        connection, cursor = create_db_connection(db_name)
        cursor.execute(query)
        commit_connection(connection, cursor)
    except Exception as e:
        LOG.info(f"control log insertion error: {e}")


def process(db_name, query_message):
    FOOD = ['sandwich', 'bread', 'meat', 'cheese', 'ham', 'omelette', 'food', 'meal']
    SERVICE = ['waiter', 'service', 'table']
    try:
        log_data = query_db(query_message)
        
        for row in log_data:
            # row = log_data[0]
            uuid = row[0]
            timestamp = row[1]
            message = row[2]
    
            food_score = 0
            service_score = 0
            num_lemm = 0
            num_char = len(message)
            tokens = nlp(message)
            for token in tokens:
                lemmas = token.lemma_
                ent = token.ent_type_
                if lemmas in FOOD and lemmas not in stop_words and lemmas not in punctuations:
                    food_score += 1
                elif (lemmas in SERVICE or ent == 'MONEY')  and lemmas not in stop_words and lemmas not in punctuations:
                    service_score += 1
                num_lemm += 1
            category = get_category(food_score, service_score)
            dynamic_insert(db_name, (uuid, timestamp, message, category, num_lemm, num_char))
            update_control_table(db_name, uuid)
    except Exception as e:
        LOG.info(f"error processing the message: {e}")

def update_control_table(db_name: str, uuid:str) -> None:
    """

    Parameters
    ----------
    db_name : str
        DESCRIPTION.
    uuid : str
        DESCRIPTION.

    Returns
    -------
    None
        DESCRIPTION.

    """
    query = """    
    UPDATE proc_log
    SET proc_time = CURRENT_TIMESTAMP
    WHERE uuid = ? ;
    """
    connection, cursor = create_db_connection(db_name)
    cursor.execute(query, (uuid, ))
    commit_connection(connection, cursor)


def get_category(food_score:int, service_score:int) -> str:
    """

    Parameters
    ----------
    food_score : int
        DESCRIPTION.
    service_score : int
        DESCRIPTION.

    Returns
    -------
    str
        DESCRIPTION.

    """
    if food_score > service_score:
        category = "SERVICE"
    elif food_score > 0:
        category = "FOOD"
    else:
        category = "GENERAL"
    return category


if __name__ == "__main__":
    db_name = "sup-san-reviews.db"
    create_proc_messages_query = """CREATE TABLE IF NOT EXISTS proc_messages(
                                                      [uuid] TEXT PRIMARY KEY,
                                                    [timestamp] timestamp,
                                                      [message] TEXT,
                                                      [category] TEXT,
                                                      [num_lemm] INTEGER,
                                                      [num_char] INTEGER,
                                                      UNIQUE (uuid)
                                                    )"""
    create_proc_log_query = """CREATE TABLE IF NOT EXISTS proc_log(
                                                      [uuid] TEXT PRIMARY KEY,
                                                      [proc_time] timestamp,
                                                      UNIQUE (uuid)
                                                    )"""
    create_table_query = [create_proc_messages_query, create_proc_log_query]
    create_table(db_name, create_table_query)

    control_log_query = '''
        INSERT INTO proc_log (uuid)
        SELECT r.uuid 
        FROM raw_messages r
        LEFT JOIN proc_log p
        	ON r.uuid=p.uuid
        WHERE p.uuid IS NULL;
    '''
    control_log_table(db_name, control_log_query)

    query_message = """
        SELECT * FROM raw_messages WHERE uuid IN (SELECT uuid FROM proc_log WHERE proc_time IS NULL)
    """
    process(db_name, query_message)
    
    LOG.info("End of Process")


