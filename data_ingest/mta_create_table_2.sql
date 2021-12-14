-- CONNECT TO DATABASE
USE {YOUR_DATABASE};

-- CREATE EXTERNAL TABLE turnstile_group_by_station
CREATE EXTERNAL TABLE turnstile_group_by_station(station STRING, log_date STRING, entries_count INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE LOCATION 'output/mta/by_station/';

-- SHOW FIRST 30 RECORDS
SELECT * FROM turnstile_group_by_station WHERE 1=1 LIMIT 30;

-- FIND DAILY NEW NUMBER OF ENTRIES FOR A STATION ON A DAY
SELECT x.station, x.log_date, x.entries_count - LAG(x.entries_count) OVER(PARTITION BY station ORDER BY x.log_date) AS entries_count FROM turnstile_group_by_station x WHERE entries_count IS NOT NULL LIMIT 30;
