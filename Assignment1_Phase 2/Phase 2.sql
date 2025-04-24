-- Drop the database if it already exists and create a new one
DROP DATABASE IF EXISTS WorldCupDB;
CREATE DATABASE WorldCupDB;
USE WorldCupDB;

-- Table 1: Players
CREATE TABLE Players (
    PID INT PRIMARY KEY,
    FullName VARCHAR(100),
    Fname VARCHAR(50),
    Lname VARCHAR(50),
    BirthDate DATE,
    Country VARCHAR(50),
    Height DECIMAL(5,2),
    Club VARCHAR(100),
    Position VARCHAR(50),
    gamesPlayed INT,
    isCaptain BOOLEAN
);

-- Table 2: Player_Cards
CREATE TABLE Player_Cards (
    PID INT PRIMARY KEY,
    no_of_yellow_cards INT,
    no_of_red_cards INT,
    FOREIGN KEY (PID) REFERENCES Players(PID) ON DELETE CASCADE
);

-- Table 3: Player_Assists_Goals
CREATE TABLE Player_Assists_Goals (
    PID INT PRIMARY KEY,
    no_of_matches INT,
    goals INT,
    assists INT,
    minutes_played INT,
    FOREIGN KEY (PID) REFERENCES Players(PID) ON DELETE CASCADE
);

-- Table 4: Country
CREATE TABLE Country (
    country_name VARCHAR(50) PRIMARY KEY,
    population BIGINT,
    world_cups_won INT,
    coach VARCHAR(100),
    capital VARCHAR(50)
);

-- Table 5: Worldcup_History
CREATE TABLE Worldcup_History (
    year INT PRIMARY KEY,
    host_country VARCHAR(50),
    winner VARCHAR(50),
    runner_up VARCHAR(50),
    final_score VARCHAR(10),
    FOREIGN KEY (winner) REFERENCES Country(country_name) ON DELETE SET NULL,
    FOREIGN KEY (runner_up) REFERENCES Country(country_name) ON DELETE SET NULL
);

-- Table 6: Match_Results
CREATE TABLE Match_Results (
    match_id INT PRIMARY KEY,
    match_date DATE,
    match_start_time TIME,
    team1 VARCHAR(50),
    team2 VARCHAR(50),
    team1_score INT,
    team2_score INT,
    stadium_name VARCHAR(100),
    city VARCHAR(50),
    FOREIGN KEY (team1) REFERENCES Country(country_name) ON DELETE CASCADE,
    FOREIGN KEY (team2) REFERENCES Country(country_name) ON DELETE CASCADE
);
ALTER TABLE Players MODIFY COLUMN isCaptain VARCHAR(5);


SELECT * FROM Country LIMIT 10;
SELECT * FROM Worldcup_History LIMIT 10;
SELECT * FROM Match_results ;
SELECT * FROM Players LIMIT 10;
SELECT * FROM Player_Cards LIMIT 10;
SELECT * FROM Player_Assists_Goals LIMIT 10;

SELECT DISTINCT winner FROM Worldcup_History;

SELECT winner, COUNT(*) AS world_cups_won 
FROM Worldcup_History 
GROUP BY winner 
ORDER BY world_cups_won DESC;

SELECT capital FROM Country WHERE population > 100 ORDER BY population ASC;

SELECT DISTINCT stadium_name FROM Match_results WHERE team1_score > 4 OR team2_score > 4;

SELECT DISTINCT city 
FROM Match_results 
WHERE stadium_name LIKE 'Estadio%';

SELECT stadium_name, COUNT(*) AS matches_hosted 
FROM Match_results 
GROUP BY stadium_name;

SELECT Fname, Lname, BirthDate 
FROM Players 
WHERE height > 198;

SELECT stadium_name, team1, team2 
FROM Match_results 
WHERE match_date BETWEEN '2014-06-20' AND '2014-06-24';

SELECT p.Fname, p.Lname, p.Position, pag.goals
FROM Players p
JOIN Player_Cards pc ON p.PID = pc.PID
JOIN Player_Assists_Goals pag ON p.PID = pag.PID
WHERE p.isCaptain = "TRUE"
AND (pc.no_of_yellow_cards > 2 OR pc.no_of_red_cards > 1);
