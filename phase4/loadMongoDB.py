"""
Document Structure (per group schema design):
{
    _id:                      String,   // track_id
    track_name:               String,
    track_artist:             String,
    track_popularity:         Number,
    track_album_release_date: Date,
    audio_features: {
        danceability:         Number,
        speechiness:          Number
    },
    spotify_chart_entries: [
        {
            chart_date:       Date,
            rank:             Number,
            streams:          Number
        }
    ],
    billboard_runs: [
        {
            instance:         Number,
            end_date:         Date,
            best_week_position: Number,
            weeks_on_chart:   Number
        }
    ]
}

Usage
-----
1. Make sure your SSH tunnel is open in a separate terminal:
       ssh -l <CWL> -L localhost:27017:nosql.students.cs.ubc.ca:27017 remote.students.cs.ubc.ca
2. Update CWL and SNUM below with your own CWL and Student Number
3. Place the three CSVs in the same folder as this script (or update DATA_DIR)
4. Run: python3 loadMongoDB.py in terminal

"""

import os
import pymongo
import pandas as pd
from datetime import datetime


CWL  = "haydenh7"  # your CWL username
SNUM = "55946628"  # 8-digit student number

DATA_DIR = os.getcwd()

SPOTIFY_SONGS_CSV   = os.path.join(DATA_DIR, "spotify_songs.csv")
CHARTS_CSV          = os.path.join(DATA_DIR, "charts.csv")
BILLBOARD_WEEKS_CSV = os.path.join(DATA_DIR, "billboard_weeks.csv")


def parse_date(date_str: str) -> datetime:
    
    # parse every date string format to datetime object for mongoDB
    
    date_str = str(date_str).strip()
    if len(date_str) == 4:        # e.g. '2013'
        return datetime.strptime(date_str, "%Y")
    elif len(date_str) == 7:      # e.g. '1967-09'
        return datetime.strptime(date_str, "%Y-%m")
    else:                         # e.g. '2019-06-28'
        return datetime.strptime(date_str, "%Y-%m-%d")

def connect(cwl: str, snum: str) -> pymongo.collection.Collection:

    # connect to server through ssh tunnel
    
    connection_string = f"mongodb://{cwl}:a{snum}@localhost:27017/{cwl}"
    client = pymongo.MongoClient(connection_string)
    db = client[cwl]
    collection = db["songs"]
    print(f"[OK] Connected to MongoDB – database: '{cwl}', collection: 'songs'")
    return collection


def load_csvs() -> tuple:

    songs_df     = pd.read_csv(SPOTIFY_SONGS_CSV)
    charts_df    = pd.read_csv(CHARTS_CSV)
    billboard_df = pd.read_csv(BILLBOARD_WEEKS_CSV)

    print(f"[OK] Loaded CSVs:")
    print(f"     spotify_songs   : {len(songs_df)} rows")
    print(f"     charts          : {len(charts_df)} rows")
    print(f"     billboard_weeks : {len(billboard_df)} rows")
    return songs_df, charts_df, billboard_df


def build_documents(songs_df, charts_df, billboard_df) -> list:
    
    # build_documents combines the three dataframes into a list of mongoDB documents
    
    # group chart entries: {track_id: [ {chart_date, rank, streams}, ... ]}
    
    charts_grouped = {}
    for _, row in charts_df.iterrows():
        tid = row["track_id"]
        entry = {
            "chart_date": parse_date(row["date"]),
            "rank":        int(row["rank"]),
            "streams":     int(float(row["streams"]))
        }
        charts_grouped.setdefault(tid, []).append(entry)

    # group billboard runs: {track_id: [ {instance, end_date, ...}, ... ]}
    
    billboard_grouped = {}
    for _, row in billboard_df.iterrows():
        tid = row["track_id"]
        run = {"instance":           int(row["instance"]),
               "end_date":           parse_date(row["end_date"]),
               "best_week_position": int(row["best_week_position"]),
               "weeks_on_chart":     int(row["weeks_on_chart"])}
        billboard_grouped.setdefault(tid, []).append(run)

    # build one document per song
    
    documents = []
    for _, row in songs_df.iterrows():
        tid = row["track_id"]
        doc = {
            "_id": tid,
            "track_name": str(row["track_name"]),
            "track_artist": str(row["track_artist"]),
            "track_popularity": int(row["track_popularity"]),
            "track_album_release_date": parse_date(row["track_album_release_date"]),
            "audio_features": {"danceability": float(row["danceability"]), 
                               "speechiness":  float(row["speechiness"])},
            "spotify_chart_entries": charts_grouped.get(tid, []),
            "billboard_runs": billboard_grouped.get(tid, [])
        }
        documents.append(doc)

    print(f"[OK] Built {len(documents)} documents")
    return documents


def insert_documents(collection: pymongo.collection.Collection,
                     documents: list) -> None:
    collection.drop()
    print("Dropped existing 'songs' collection (if any)")
    result = collection.insert_many(documents)
    print(f"Inserted {len(result.inserted_ids)} documents into 'songs'")


def verify(collection: pymongo.collection.Collection) -> None:
    """Print a quick sanity check after loading."""
    total = collection.count_documents({})
    with_charts    = collection.count_documents(
        {"spotify_chart_entries": {"$not": {"$size": 0}}}
    )
    with_billboard = collection.count_documents(
        {"billboard_runs": {"$not": {"$size": 0}}}
    )
    sample = collection.find_one({"spotify_chart_entries": {"$not": {"$size": 0}}},
                                  {"track_name": 1, "spotify_chart_entries": {"$slice": 1}})

    print()
    print("=== Verification ===")
    print(f"Total documents        : {total}")
    print(f"With chart entries     : {with_charts}")
    print(f"With billboard runs    : {with_billboard}")
    print(f"Sample document (name) : {sample.get('track_name') if sample else 'N/A'}")


if __name__ == "__main__":
    # Validate credentials are set
    if SNUM == "your_student_num":
        print("[ERROR] Please update CWL and SNUM in the configuration section.")
        exit(1)

    collection           = connect(CWL, SNUM)
    songs_df, charts_df, billboard_df = load_csvs()
    documents            = build_documents(songs_df, charts_df, billboard_df)
    insert_documents(collection, documents)
    verify(collection)
