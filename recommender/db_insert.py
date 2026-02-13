## This script is used to insert data into the database
import os
import json
from dotenv import load_dotenv
from datasets import load_dataset
import pandas as pd
import psycopg2

load_dotenv()

from utils import fast_pg_insert
# TODO: Read the embedding files

folder_path = "../embedding"
embeddings = {}
podcasts = {}

for filename in os.listdir(folder_path):
    full_path = os.path.join(folder_path, filename)
    with open(full_path) as f:
        for line in f:
            obj = json.loads(line)
            embeddings[obj["custom_id"]] = obj["response"]["body"]["data"][0]["embedding"]


# TODO: Read documents files
folder_path = "../documents"
rows = []
podcast_rows = []

for filename in os.listdir(folder_path):
    full_path = os.path.join(folder_path, filename)
    with open(full_path) as f:
        for line in f:
            obj = json.loads(line)
            podcast_id = obj["body"]["metadata"]["podcast_id"]
            podcast_title = obj["body"]["metadata"]["title"]
            if podcast_id not in podcasts:
                podcasts[podcast_id] = podcast_title
            rows.append({
            "id": obj["custom_id"],
            "start_time": obj["body"]["metadata"]["start_time"],
            "end_time": obj["body"]["metadata"]["stop_time"],
            "content": obj["body"]["input"],
            "embedding": embeddings[obj["custom_id"]],
            "podcast_id": podcast_id
        })

df_segments = pd.DataFrame(rows)
print(df_segments.head())

for podcast_id, title in podcasts.items():
    podcast_rows.append({
        "id": podcast_id,
        "title": title
    })

df_podcast = pd.DataFrame(podcast_rows)
print(df_podcast.head())

# HINT: In addition to the embedding and document files you likely need to load the raw data via the hugging face datasets library
ds = load_dataset("Whispering-GPT/lex-fridman-podcast")


# TODO: Insert into postgres
# HINT: use the recommender.utils.fast_pg_insert function to insert data into the database
# otherwise inserting the 800k documents will take a very, very long time
CONNECTION = os.getenv("DB_URL")


fast_pg_insert(
    df_podcast,
    CONNECTION,
    "podcast",
    ["id", "title"]
)


fast_pg_insert(
    df_segments,
    CONNECTION,
    "podcast_segment",
    ["id", "start_time", "end_time", "content", "embedding", "podcast_id"]
)