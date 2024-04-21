#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Jan 12 19:52:58 2024

@author: johnomole
"""
import logging
import os
import sqlite3
import sys
import csv
logging.basicConfig(format="%(asctime)s %(name)s %(levelname)-10s %(message)s")
LOG = logging.getLogger("Ingestion Pipeline")
LOG.setLevel(os.environ.get("LOG_LEVEL", logging.DEBUG))

"""
* The script accepts one parameter, a file name containing messages to ingest
* The file is in csv format (with headers) and has three columns: timestamp, uuid, message
* The script will check if table raw_messages (with the suitable schema to store the information in the files) 
exist and if it doesn’t it should create it. 
The table is in ddbb sup-san-reviews that should be created by the script if it does not exist.
"""


def create_db_connection(db_name):
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


def create_table(db_name, create_table_query):
    """

    Parameters
    ----------
    db_name : TYPE
        DESCRIPTION.
    create_table_query : TYPE
        DESCRIPTION.

    Returns
    -------
    connection : TYPE
        DESCRIPTION.
    cursor : TYPE
        DESCRIPTION.

    """
    try:
        connection, cursor = create_db_connection(db_name)
        cursor.execute(create_table_query)
    except Exception as e:
        LOG.info(f"db creation error: {e}")
    return connection, cursor


def dynamic_insert(file_name:str, insert_query:str, connection, cursor) -> None:
    """

    Parameters
    ----------
    file_name : str
        DESCRIPTION.
    insert_query : str
        DESCRIPTION.
    connection : sqlite
        DESCRIPTION.
    cursor : sqlite
        DESCRIPTION.

    Returns
    -------
    None
        DESCRIPTION.

    """
    try:
        with open(f"{file_name}", "r") as file:
            reader = csv.reader(file)
            next(reader)
            for row in reader:
                cursor.execute(insert_query, row)
    except Exception as e:
        LOG.info(f"inserting into table error: {e}")

if __name__ == "__main__":
    file_name = sys.argv[1]
    # file_name ='review_card.csv'
    db_name = "sup-san-reviews.db"
    create_table_query = """CREATE TABLE IF NOT EXISTS raw_messages(
                                                      [uuid] TEXT PRIMARY KEY,
                                                    [timestamp] timestamp,
                                                      [message] TEXT,
                                                      UNIQUE (uuid)
                                                    )"""
    insert_query = """INSERT OR REPLACE INTO raw_messages(uuid, timestamp, message) \
                        VALUES (?, ?, ?)"""
    connection, cursor = create_table(db_name, create_table_query)
    dynamic_insert(file_name, insert_query, connection, cursor)
    commit_connection(connection, cursor)


    LOG.info("End of Process")
    
