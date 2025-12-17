-- Movie with highest average rating
SELECT m.title, AVG(r.rating) AS avg_rating
FROM movies m
JOIN ratings r ON m.movie_id = r.movie_id
GROUP BY m.movie_id
ORDER BY avg_rating DESC
LIMIT 1;

-- Top 5 genres by average rating
SELECT m.genres, AVG(r.rating) AS avg_rating
FROM movies m
JOIN ratings r ON m.movie_id = r.movie_id
GROUP BY m.genres
ORDER BY avg_rating DESC
LIMIT 5;

-- Director with most movies
SELECT director, COUNT(*) AS total_movies
FROM movies
WHERE director IS NOT NULL
GROUP BY director
ORDER BY total_movies DESC
LIMIT 1;

-- Average rating by release year
SELECT m.release_year, AVG(r.rating) AS avg_rating
FROM movies m
JOIN ratings r ON m.movie_id = r.movie_id
GROUP BY m.release_year
ORDER BY m.release_year;
