Movie Data Pipeline

Project Overview:

The Movie Data Pipeline project is a small end-to-end data engineering assignment.The goal of this project is to ingest movie data from multiple sources, clean and transform it, store it in a relational database, and answer analytical questions using SQL.

This project demonstrates basic ETL concepts, API integration, database design, and analytical querying.

Objective:

1.Design and build a small data pipeline
2.Ingest movie data from two different sources
3.Clean and transform the data
4.Load the processed data into a database
5.Answer analytical questions using SQL

Data Sources:

1.CSV Files
1.1. movies.csv – contains movie details such as title and genres

    1.2. ratings.csv – contains user ratings for movies

2.OMDb API
2.1.Used to enrich movie data with additional details such as:
Director
Release year
Runtime
Plot
Language

Technology Stack:
1.Python
2.Pandas
3.MySQL
4.OMDb API
5.Git & GitHub

Project Structure:
movie-data-pipeline/
etl.py
schema.sql
queries.sql
data/
movies.csv
ratings.csv
db_config.py (ignored)
README.md

Database Schema

The project uses two tables:
1.movies
movie_id (Primary Key)
title
genres
director
release_year
runtime
plot
language
release_decade
2.ratings
user_id
movie_id (Foreign Key)
rating
timestamp

This design avoids data duplication and supports efficient analytical queries.

ETL Pipeline Explanation:
1.Extract:
Read movie and rating data from CSV files using Pandas
2.Transform:
Clean movie titles
Extract release year from title
Fetch additional movie details from OMDb API
Handle missing or unavailable data safely
Create derived fields such as release decade
3.Load:
Load cleaned and enriched data into MySQL
Use INSERT IGNORE to avoid duplicate records
Ensure the ETL process is idempotent

Handling Errors and Missing Data:
API calls use timeouts to prevent hanging requests
Exceptions are handled gracefully
Missing API values are stored as NULL
The pipeline continues execution even if some API calls fail

How to Run the Project:
1.Create the database tables:
-->SOURCE schema.sql;
2.Run the ETL pipeline:
-->python etl.py
3.Execute analytical queries:
-->SOURCE queries.sql;

Challenges Faced:
1.Handling API request failures
2.Managing API rate limits
3.Cleaning inconsistent movie titles
4.Ensuring database connections were stable

Conclusion:

This project demonstrates the fundamentals of data engineering, including ETL pipeline design, API integration, relational data modeling, and SQL-based analytics. It also highlights handling real-world issues such as missing data and external API reliability.
