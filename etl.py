import pandas as pd          # Read CSV files
import requests              # Call OMDb API
import mysql.connector       # Connect Python to MySQL
import time                  # Add delay between API calls

from db_config import DB_HOST, DB_USER, DB_PASSWORD, DB_NAME, API_key

OMDB_API_KEY = None          # API disabled after data ingestion
OMDB_URL = "http://www.omdbapi.com/"


# ---------------- DATABASE CONNECTION ----------------
def get_db_connection():
    # Create and return MySQL database connection
    return mysql.connector.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME,
        port=3306
    )


# ---------------- OMDb API CALL ----------------
def fetch_omdb_data(title, year):
    # Remove year from movie title for better API match
    clean_title = title.split("(")[0].strip()

    params = {
        "t": clean_title,
        "apikey": OMDB_API_KEY
    }

    if year:
        params["y"] = year

    try:
        # Call OMDb API with timeout
        response = requests.get(OMDB_URL, params=params, timeout=5)
        data = response.json()
    except Exception:
        # If API fails, return empty values
        return (None, year, None, None, None)

    if data.get("Response") == "True":
        # Convert runtime to integer
        runtime = None
        if data.get("Runtime") and data["Runtime"] != "N/A":
            runtime = int(data["Runtime"].split()[0])

        # Convert year to integer
        release_year = None
        if data.get("Year", "").isdigit():
            release_year = int(data.get("Year"))

        return (
            data.get("Director"),
            release_year,
            runtime,
            data.get("Plot"),
            data.get("Language")
        )

    # If movie not found in API
    return (None, year, None, None, None)


# ---------------- MAIN ETL ----------------
def run_etl():
    print("ETL started...")

    # Read CSV files
    movies_df = pd.read_csv("data/movies.csv").head(40)   # Limited for safety
    ratings_df = pd.read_csv("data/ratings.csv")

    # Open database connection
    conn = get_db_connection()
    cursor = conn.cursor()

    # -------- MOVIES LOAD --------
    for _, row in movies_df.iterrows():
        movie_id = int(row["movieId"])
        title = row["title"]
        genres = row["genres"]

        # Extract year from title
        year = None
        if "(" in title and ")" in title:
            year_part = title.split("(")[-1].replace(")", "")
            if year_part.isdigit():
                year = int(year_part)

        # Fetch enriched data from API
        director, release_year, runtime, plot, language = fetch_omdb_data(title, year)

        # Create decade feature
        release_decade = None
        if release_year:
            release_decade = f"{(release_year // 10) * 10}s"

        # Insert movie data (avoid duplicates)
        cursor.execute(
            """
            INSERT IGNORE INTO movies
            (movie_id, title, genres, director, release_year, runtime, plot, language, release_decade)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """,
            (movie_id, title, genres, director, release_year, runtime, plot, language, release_decade)
        )

        time.sleep(0.2)  # Avoid API rate limit

    # -------- RATINGS LOAD --------
    for _, row in ratings_df.iterrows():
        # Insert rating data
        cursor.execute(
            """
            INSERT IGNORE INTO ratings
            (user_id, movie_id, rating, timestamp)
            VALUES (%s,%s,%s,%s)
            """,
            (row["userId"], row["movieId"], row["rating"], row["timestamp"])
        )

    # Save changes and close connection
    conn.commit()
    cursor.close()
    conn.close()

    print("ETL completed successfully.")


# ---------------- RUN SCRIPT ----------------
if __name__ == "__main__":
    run_etl()
