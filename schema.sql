CREATE TABLE movies (
    movie_id INT PRIMARY KEY,
    title VARCHAR(255) NOT NULL,
    genres VARCHAR(255),
    director VARCHAR(255),
    release_year INT,
    runtime INT,
    plot TEXT,
    language VARCHAR(100),
    release_decade VARCHAR(10)
);

CREATE TABLE ratings (
    user_id INT,
    movie_id INT,
    rating DECIMAL(2, 1),
    timestamp BIGINT,
    PRIMARY KEY (user_id, movie_id),
    FOREIGN KEY (movie_id) REFERENCES movies (movie_id)
);