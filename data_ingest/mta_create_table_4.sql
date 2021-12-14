-- CONNECT TO DATABASE
USE {YOUR_DATABASE};

-- DROP EXISTING TABLE AND CREATE A NEW ONE
DROP TABLE IF EXISTS turnstile_group_by_station_remove_outliers;
CREATE EXTERNAL TABLE turnstile_group_by_station_remove_outliers(station STRING, log_date STRING, entries_count INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE LOCATION 'output/mta/by_station_remove_outliers';

-- SHOW FIRST 30 RECORDS
SELECT * FROM turnstile_group_by_station_remove_outliers WHERE 1=1 LIMIT 30;

-- FIND DAILY NEW NUMBER OF ENTRIES FOR ON A DAY
SELECT log_date, SUM(entries_count) AS entries_count FROM turnstile_group_by_station_remove_outliers GROUP BY log_date ORDER BY log_date LIMIT 30;
