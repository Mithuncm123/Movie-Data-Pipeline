import pandas as pd
import requests
import mysql.connector
from db_config import DB_HOST, DB_USER, DB_PASSWORD, DB_NAME

OMDB_API_KEY = "YOUR_OMDB_API_KEY"
OMDB_URL = "http://www.omdbapi.com/"

def get_db_connection():
    return mysql.connector.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME
    )

def fetch_omdb_data(title, year):
    params = {
        "t": title,
        "y": year,
        "apikey": OMDB_API_KEY
    }
    response = requests.get(OMDB_URL, params=params)
    data = response.json()

    if data.get("Response") == "True":
        runtime = None
        if data.get("Runtime") and data["Runtime"] != "N/A":
            runtime = int(data["Runtime"].split()[0])

        return (
            data.get("Director"),
            int(data.get("Year")) if data.get("Year", "").isdigit() else None,
            runtime,
            data.get("Plot"),
            data.get("Language")
        )
    else:
        return (None, year, None, None, None)

def run_etl():
    movies_df = pd.read_csv("data/movies.csv")
    ratings_df = pd.read_csv("data/ratings.csv")

    conn = get_db_connection()
    cursor = conn.cursor()

    for _, row in movies_df.iterrows():
        movie_id = int(row["movieId"])
        title = row["title"]
        genres = row["genres"]

        year = None
        if "(" in title and ")" in title:
            year_part = title.split("(")[-1].replace(")", "")
            if year_part.isdigit():
                year = int(year_part)

        director, release_year, runtime, plot, language = fetch_omdb_data(title, year)

        release_decade = None
        if release_year:
            release_decade = str((release_year // 10) * 10) + "s"

        cursor.execute(
            """
            INSERT IGNORE INTO movies
            (movie_id, title, genres, director, release_year, runtime, plot, language, release_decade)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """,
            (movie_id, title, genres, director, release_year, runtime, plot, language, release_decade)
        )

    for _, row in ratings_df.iterrows():
        cursor.execute(
            """
            INSERT IGNORE INTO ratings
            (user_id, movie_id, rating, timestamp)
            VALUES (%s,%s,%s,%s)
            """,
            (row["userId"], row["movieId"], row["rating"], row["timestamp"])
        )

    conn.commit()
    cursor.close()
    conn.close()

if __name__ == "__main__":
    run_etl()
