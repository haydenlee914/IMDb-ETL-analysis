### Creating the Fact table for Business Question 1

CREATE TABLE RelationshipFact (
    relationshipId INT AUTO_INCREMENT PRIMARY KEY,
    person1_id VARCHAR(255),
    person1_professionId INT,
    person2_id VARCHAR(255),
    person2_professionId INT,
    movieCount INT,
    averageRating FLOAT,
    averageNumVotes INT
);

INSERT INTO RelationshipFact (person1_id, person1_professionId, person2_id, person2_professionId, movieCount, averageRating, averageNumVotes)
SELECT
    P1.personId AS person1_id,
    PR1.professionId AS person1_Id,
    P2.personId AS person2_id,
    PR2.professionId AS person2_Id,
    COUNT(MP.movieId) AS movieCount,
    AVG(M.averageRating) AS averageRating,
    AVG(M.numVotes) AS averageNumVotes
FROM
    MoviePerson MP
JOIN Person P1 ON MP.personId = P1.personId
JOIN PersonProfession PP1 ON P1.personId = PP1.personId
JOIN Profession PR1 ON PP1.professionId = PR1.professionId
JOIN MoviePerson MP2 ON MP.movieId = MP2.movieId AND MP.personId != MP2.personId
JOIN Person P2 ON MP2.personId = P2.personId
JOIN PersonProfession PP2 ON P2.personId = PP2.personId
JOIN Profession PR2 ON PP2.professionId = PR2.professionId
JOIN Movie M ON MP.movieId = M.movieId
GROUP BY
    P1.personId,
    PR1.professionId,
    P2.personId,
    PR2.professionId;


## Procedure for 

DELIMITER //

CREATE PROCEDURE FindProfessionCollaborations(
    IN primary_person_name VARCHAR(255),
    IN primary_profession_name VARCHAR(255),
    IN secondary_profession_name VARCHAR(255)
)
BEGIN
    DECLARE primary_person_id VARCHAR(255);
    DECLARE primary_profession_id INT;
    DECLARE secondary_profession_id INT;

    -- Find the professionId for the primary profession (e.g., 'actor')
    SELECT professionId INTO primary_profession_id
    FROM Profession
    WHERE profession = primary_profession_name;

    -- Find the professionId for the secondary profession (e.g., 'director')
    SELECT professionId INTO secondary_profession_id
    FROM Profession
    WHERE profession = secondary_profession_name;

    -- Find the primary person's ID (e.g., the actor's ID)
    SELECT P.personId INTO primary_person_id
    FROM Person P
    JOIN PersonProfession PP ON P.personId = PP.personId
    WHERE P.primaryName = primary_person_name AND PP.professionId = primary_profession_id;

    -- Retrieve collaboration information from FactTable
    SELECT DISTINCT FT.person2_Id, FT.movieCount, FT.averageRating, FT.averageNumVotes
    FROM RelationshipFact FT
    JOIN PersonProfession PP ON FT.person2_Id = PP.personId
    WHERE FT.person1_Id = primary_person_id AND PP.professionId = secondary_profession_id;
END //

DELIMITER ;

CALL FindProfessionCollaborations('Tom Hanks', 'stunts', 'producer');



# Creating table PerformanceFact;

CREATE TABLE PerformanceFact (
    performanceId INT AUTO_INCREMENT PRIMARY KEY,
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

INSERT INTO PerformanceFact (
    actor_id,
    movie_id,
    genre_id,
    region_id,
    language_id,
    averageRating,
    averageNumVotes,
    movieCount,
    birthYear,
    deathYear
)
SELECT
    MP.personId AS actor_id,
    T.movieId AS movie_id,
    MG.genreId AS genre_id,
    T.regionId AS region_id,
    T.languageId AS language_id,
    ActorMovies.averageRating,
    ActorMovies.averageNumVotes,
    ActorMovies.movieCount,
    P.birthYear,
    P.deathYear
FROM
    MoviePerson MP
JOIN Person P ON MP.personId = P.personId
JOIN (
    SELECT
        MP2.personId,
        AVG(M.averageRating) AS averageRating,
        AVG(M.numVotes) AS averageNumVotes,
        COUNT(M.movieId) AS movieCount
    FROM MoviePerson MP2
    JOIN Movie M ON MP2.movieId = M.movieId
    GROUP BY MP2.personId
) ActorMovies ON MP.personId = ActorMovies.personId
JOIN Title T ON MP.movieId = T.movieId
JOIN MovieGenre MG ON T.movieId = MG.movieId
GROUP BY MP.personId, T.movieId, MG.genreId, T.regionId, T.languageId,
         ActorMovies.averageRating, ActorMovies.averageNumVotes,
         ActorMovies.movieCount, P.birthYear, P.deathYear;
