-- use the correct database
USE xz2715; 

-- create table
CREATE EXTERNAL TABLE date_trips(trip_date STRING, trip_num INT)
COMMENT 'date_trips'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE
LOCATION '{YOUR_FHV_PROF_OUTPUT_PATH}';
