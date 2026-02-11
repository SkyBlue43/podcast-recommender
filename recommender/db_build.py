## This script is used to create the tables in the database

import os
from dotenv import load_dotenv
import psycopg2

load_dotenv()

CONNECTION = os.getenv("DB_URL")

# need to run this to enable vector data type
CREATE_EXTENSION = "CREATE EXTENSION IF NOT EXISTS vector"

# TODO: Add create table statement
CREATE_PODCAST_TABLE = """
    CREATE TABLE IF NOT EXISTS podcast (
        id VARCHAR(50) PRIMARY KEY,
        title VARCHAR(255) NOT NULL
    );
"""
# TODO: Add create table statement
CREATE_SEGMENT_TABLE = """
    CREATE TABLE IF NOT EXISTS podcast_segment (
        id VARCHAR(50) PRIMARY KEY,
        start_time FLOAT NOT NULL,
        end_time FLOAT NOT NULL,
        content TEXT NOT NULL,
        embedding VECTOR(128),
        podcast_id VARCHAR(50),
        FOREIGN KEY (podcast_id) REFERENCES podcast (id)
    );
"""

conn = psycopg2.connect(CONNECTION)
# TODO: Create tables with psycopg2 (example: https://www.geeksforgeeks.org/executing-sql-query-with-psycopg2-in-python/)
cursor = conn.cursor()
cursor.execute(CREATE_EXTENSION)
cursor.execute(CREATE_PODCAST_TABLE)
cursor.execute(CREATE_SEGMENT_TABLE)

conn.commit()
conn.close()



