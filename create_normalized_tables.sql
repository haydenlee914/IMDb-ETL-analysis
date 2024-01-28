CREATE TABLE Region (
    regionId INT PRIMARY KEY,
    region VARCHAR(45)
);

CREATE TABLE Language (
    languageId INT PRIMARY KEY,
    language VARCHAR(45)
);

CREATE TABLE Type (
    typeId INT PRIMARY KEY,
    type VARCHAR(255)
);

CREATE TABLE Genre (
    genreId INT PRIMARY KEY,
    genre VARCHAR(255)
);

CREATE TABLE Profession (
    professionId INT PRIMARY KEY,
    profession VARCHAR(255)
);

CREATE TABLE Person (
    personId VARCHAR(10) PRIMARY KEY,
    primaryName VARCHAR(255),
    birthYear TEXT,
    deathYear TEXT
);

CREATE TABLE Movie (
    movieId VARCHAR(10) PRIMARY KEY,
    movieType INT,
    primaryTitle VARCHAR(255),
    originalTitle VARCHAR(255),
    isAdult TINYINT(1),
    startYear TEXT,
    endYear TEXT,
    runtimeMinutes INT,
    averageRating FLOAT,
    numVotes INT,

    FOREIGN KEY (movieType) REFERENCES Type(typeId)
);

CREATE TABLE Title (
    titleId INT PRIMARY KEY,
    movieId VARCHAR(45),
    title VARCHAR(45),
    regionId INT,
    languageId INT,
    isOriginalTitle TINYINT(1),
    FOREIGN KEY (movieId) REFERENCES Movie(movieId),
    FOREIGN KEY (regionId) REFERENCES Region(regionId),
    FOREIGN KEY (languageId) REFERENCES Language(languageId)
);

CREATE TABLE MovieGenre (
    movieId VARCHAR(10),
    genreId INT,
    PRIMARY KEY (movieId, genreId),
    FOREIGN KEY (movieId) REFERENCES Movie(movieId),
    FOREIGN KEY (genreId) REFERENCES Genre(genreId)
);

CREATE TABLE MoviePerson (
    movieId VARCHAR(10),
    personId VARCHAR(10),
    PRIMARY KEY (movieId, personId),
    FOREIGN KEY (movieId) REFERENCES Movie(movieId),
    FOREIGN KEY (personId) REFERENCES Person(personId)
);

CREATE TABLE PersonProfession (
    personId VARCHAR(10),
    professionId INT,
    PRIMARY KEY (personId, professionId),
    FOREIGN KEY (personId) REFERENCES Person(personId),
    FOREIGN KEY (professionId) REFERENCES Profession(professionId)
);