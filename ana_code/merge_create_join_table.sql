-- DROP TABLE IF EXISTS
DROP TABLE IF EXISTS project;

-- CREATE JOIN TABLE
CREATE TABLE project(log_date STRING, new_covid INT, new_trips INT, new_mta INT);

-- LOAD DATA INTO JOIN TABLE
INSERT INTO project
SELECT date_trips.trip_date AS log_date, covid.new_positive AS new_covid, date_trips.trip_num AS new_trips, mta.new_riders AS new_mta
FROM date_trips, covid, mta
WHERE date_trips.trip_date = covid.log_date AND covid.log_date = mta.log_date AND covid.county = "New York";

-- SHOW FIRST 30 RECORDS
SELECT * FROM project WHERE 1=1 LIMIT 30;
