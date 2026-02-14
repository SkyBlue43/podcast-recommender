## This script is used to query the database
import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

CONNECTION = os.getenv("DB_URL")

query_1 = """
    SELECT p.title, ps.id, ps.content, ps.start_time, ps.end_time, ps.embedding <-> (
        SELECT embedding 
        FROM podcast_segment
        WHERE id = '267:476') AS Distance
    FROM podcast_segment ps
    JOIN podcast p 
        ON p.id = ps.podcast_id
    WHERE ps.id != '267:476'
    ORDER BY Distance 
    LIMIT 5;
"""

query_2 = """
    SELECT p.title, ps.id, ps.content, ps.start_time, ps.end_time, ps.embedding <-> (
        SELECT embedding 
        FROM podcast_segment
        WHERE id = '267:476') AS Distance
    FROM podcast_segment ps
    JOIN podcast p 
        ON p.id = ps.podcast_id
    WHERE ps.id != '267:476'
    ORDER BY Distance DESC
    LIMIT 5;
"""

query_3 = """
    SELECT p.title, ps.id, ps.content, ps.start_time, ps.end_time, ps.embedding <-> (
        SELECT embedding 
        FROM podcast_segment
        WHERE id = '48:511') AS Distance
    FROM podcast_segment ps
    JOIN podcast p 
        ON p.id = ps.podcast_id
    WHERE ps.id != '48:511'
    ORDER BY Distance 
    LIMIT 5;
"""

query_4 = """
    SELECT p.title, ps.id, ps.content, ps.start_time, ps.end_time, ps.embedding <-> (
        SELECT embedding 
        FROM podcast_segment
        WHERE id = '51:56') AS Distance
    FROM podcast_segment ps
    JOIN podcast p 
        ON p.id = ps.podcast_id
    WHERE ps.id != '51:56'
    ORDER BY Distance 
    LIMIT 5;
"""

query_5a = """
    WITH episode_embeddings AS (
        SELECT podcast_id, AVG(embedding) as avg_embedding
        FROM podcast_segment
        GROUP BY podcast_id
        )
    SELECT 
        p.title,
        e.avg_embedding <-> (
            SELECT embedding 
            FROM podcast_segment 
            WHERE id = '267:476'
        ) AS distance
    FROM episode_embeddings e
    JOIN podcast p
        ON p.id = e.podcast_id
    WHERE e.podcast_id != (
            SELECT podcast_id 
            FROM podcast_segment 
            WHERE id = '267:476'
    )
    ORDER BY distance
    LIMIT 5;
"""

query_5b = """
    WITH episode_embeddings AS (
        SELECT podcast_id, AVG(embedding) as avg_embedding
        FROM podcast_segment
        GROUP BY podcast_id
        )
    SELECT 
        p.title,
        e.avg_embedding <-> (
            SELECT embedding 
            FROM podcast_segment 
            WHERE id = '48:511'
        ) AS distance
    FROM episode_embeddings e
    JOIN podcast p
        ON p.id = e.podcast_id
    WHERE e.podcast_id != (
            SELECT podcast_id 
            FROM podcast_segment 
            WHERE id = '48:511'
    )
    ORDER BY distance
    LIMIT 5;
"""

query_5c = """
    WITH episode_embeddings AS (
        SELECT podcast_id, AVG(embedding) as avg_embedding
        FROM podcast_segment
        GROUP BY podcast_id
        )
    SELECT 
        p.title,
        e.avg_embedding <-> (
            SELECT embedding 
            FROM podcast_segment 
            WHERE id = '51:56'
        ) AS distance
    FROM episode_embeddings e
    JOIN podcast p
        ON p.id = e.podcast_id
    WHERE e.podcast_id != (
            SELECT podcast_id 
            FROM podcast_segment 
            WHERE id = '51:56'
    )
    ORDER BY distance
    LIMIT 5;
"""

query_6 = """
    WITH episode_embeddings AS (
    SELECT 
        podcast_id,
        AVG(embedding) AS avg_embedding
    FROM podcast_segment
    GROUP BY podcast_id
)

    SELECT 
        p.title,
        e.avg_embedding <-> (
            SELECT avg_embedding
            FROM episode_embeddings
            WHERE podcast_id = 'VeH7qKZr0WI'
        ) AS distance
    FROM episode_embeddings e
    JOIN podcast p
        ON p.id = e.podcast_id
    WHERE e.podcast_id != 'VeH7qKZr0WI'
    ORDER BY distance
    LIMIT 5;
"""

def run_query(cur, query, label):
    print(f"\n===== {label} =====")
    cur.execute(query)
    rows = cur.fetchall()

    for row in rows:
        print(row)


if __name__ == "__main__":
    conn = psycopg2.connect(CONNECTION)
    cur = conn.cursor()

    run_query(cur, query_1, "Q1")
    run_query(cur, query_2, "Q2")
    run_query(cur, query_3, "Q3")
    run_query(cur, query_4, "Q4")
    run_query(cur, query_5a, "Q5a")
    run_query(cur, query_5b, "Q5b")
    run_query(cur, query_5c, "Q5c")
    run_query(cur, query_6, "Q6")

    cur.close()
    conn.close()