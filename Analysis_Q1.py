
# Create EMR and execute the folloiwng codes below on Hue

CREATE TABLE PerformanceFact (
    performanceId INT,
    actor_id VARCHAR(45),
    movie_id VARCHAR(45),
    genre_id INT,
    region_id INT,
    language_id INT,
    averageRating FLOAT,
    averageNumVotes INT,
    movieCount INT,
    birthYear VARCHAR(45),
    deathYear VARCHAR(45)
);
STORED AS PARQUET LOCATION 's3://groupmsba/PerformanceFact';


CREATE TABLE Person (
    personId VARCHAR(10),
    primaryName VARCHAR(255),
    birthYear VARCHAR(4),
    deathYear VARCHAR(4)
)
STORED AS PARQUET LOCATION 's3://anne-groupmsba/Person';


# Actors with the Most Diverse Genre Experience:
SELECT p.actor_id as actorId, pe.primaryname as name, COUNT(DISTINCT p.genre_id) AS unique_genres
FROM performancefact p
JOIN person pe ON p.actor_id = pe.personid
GROUP BY p.actor_id, pe.primaryname
ORDER BY unique_genres DESC
LIMIT 5;


# Top Actors by Average Rating
SELECT actor_id, AVG(averageRating) AS avg_rating
FROM PerformanceFact
GROUP BY actor_id
ORDER BY avg_rating DESC
LIMIT 10;

# Top Actors by Number of Votes
SELECT actor_id, SUM(averageNumVotes) AS total_votes
FROM PerformanceFact
GROUP BY actor_id
ORDER BY total_votes DESC
LIMIT 10;

# Actors with the Most Movie Appearances
SELECT actor_id, COUNT(DISTINCT movie_id) AS movie_appearances
FROM PerformanceFact
GROUP BY actor_id
ORDER BY movie_appearances DESC
LIMIT 10;

# Top Actors by Genre
SELECT actor_id, genre_id, AVG(averageRating) AS avg_rating
FROM PerformanceFact
GROUP BY actor_id, genre_id
ORDER BY avg_rating DESC
LIMIT 10;

# Actors Born in a Specific Range:
SELECT actor_id, birthYear
FROM PerformanceFact
WHERE birthYear BETWEEN '1980' AND '1990';

# Actors with the Most Movie Appearances in a Specific Region:
SELECT actor_id, region_id, COUNT(DISTINCT movie_id) AS movie_appearances
FROM PerformanceFact
GROUP BY actor_id, region_id
ORDER BY movie_appearances DESC
LIMIT 10;

# Actors with the Most Movie Appearances in a Specific Language:
SELECT actor_id, language_id, COUNT(DISTINCT movie_id) AS movie_appearances
FROM PerformanceFact
GROUP BY actor_id, language_id
ORDER BY movie_appearances DESC
LIMIT 10;


# Top Actors by Average Rating in Recent Years:
SELECT actor_id, AVG(averageRating) AS avg_rating
FROM PerformanceFact
WHERE deathYear IS NULL OR CAST(deathYear AS INT) > 2020
GROUP BY actor_id
ORDER BY avg_rating DESC
LIMIT 10;