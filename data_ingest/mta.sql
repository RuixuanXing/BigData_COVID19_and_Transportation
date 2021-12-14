-- CONNECT TO DATABASE
USE zl2902;

-- DROP EXISTING TABLE AND CREATE A NEW ONE
DROP TABLE IF EXISTS turnstile_group_by_station_daily_new;
CREATE EXTERNAL TABLE turnstile_group_by_station_daily_new(station STRING, log_date STRING, entries_count INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE LOCATION '/user/zl2902/final_project/mta/by_station_daily_new';

-- SHOW FIRST 30 RECORDS
SELECT * FROM turnstile_group_by_station_daily_new WHERE 1=1 LIMIT 30;

-- FIND OUTLIERS RANGE
SELECT percentile_approx(entries_count, 0.25) AS q1, percentile_approx(entries_count, 0.75) AS q3, (percentile_approx(entries_count, 0.75) - percentile_approx(entries_count, 0.25)) AS iqr FROM turnstile_group_by_station_daily_new;

-- REMOVE OUTLIERS
SELECT * FROM turnstile_group_by_station_daily_new WHERE entries_count IS NOT NULL AND entries_count > 0 AND entries_count < 28018 LIMIT 30;
