-- CONNECT TO DATABASE
USE {YOUR_DATABASE};

-- DROP TABLE IF EXISTS
DROP TABLE IF EXISTS turnstile;
DROP TABLE IF EXISTS turnstile_group_by_scp;
DROP TABLE IF EXISTS turnstile_group_by_station;
DROP TABLE IF EXISTS turnstile_group_by_date;

-- CREATE EXTERNAL TABLE
CREATE EXTERNAL TABLE turnstile(scp STRING, station STRING, line_name STRING, log_date STRING, log_time STRING, entries_count INT, exist_count INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE LOCATION '{YOUR_MTA_OUTPUT_PATH}';

-- SHOW FIRST 30 RECORDS
SELECT * FROM turnstile WHERE 1=1 LIMIT 30;

-- FIND THE TOTAL CUMULATIVE ENTRIES AND EXIST COUNT FOR A TURNSTILE AT A STATION ON A DAY
SELECT scp, station, log_date, MAX(entries_count) AS entries_count FROM turnstile GROUP BY scp, station, log_date ORDER BY scp, station, log_date LIMIT 30;

-- CREATE A NEW TABLE TO STORE PREVIOUS RESULT
CREATE TABLE turnstile_group_by_scp(scp STRING, station STRING, log_date STRING, entries_count INT);

-- INSERT INTO THE NEW TABLE
INSERT INTO turnstile_group_by_scp SELECT scp, station, log_date, MAX(entries_count) AS entries_count FROM turnstile GROUP BY scp, station, log_date ORDER BY scp, station, log_date;

-- SHOW FIRST 30 RECORDS
SELECT * FROM turnstile_group_by_scp WHERE 1=1 LIMIT 30;

-- FIND THE TOTAL CUMULATIVE ENTRIES AND EXIST COUNT FOR A STATION ON A DAY
SELECT station, log_date, SUM(entries_count) AS entries_count FROM turnstile_group_by_scp GROUP BY station, log_date ORDER BY station, log_date LIMIT 30;
