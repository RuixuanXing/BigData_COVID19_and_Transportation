-- use the correct database
USE {YOUR_DATABASE}; 

-- Analysis 1
-- people trip pattern during normal period(2020-09-01 -> 2020-11-01). 10 days where people take the least number of ubers
SELECT trip_date, trip_num FROM date_trips WHERE trip_date BETWEEN '2020-09-01' AND '2020-11-01' ORDER BY trip_num LIMIT 10; 
-- found 9 out of 10 are on Sunday, people are inclined to be dormant

-- Analysis 2
-- 10 days where people take the most number of trips
SELECT trip_date, trip_num FROM date_trips WHERE trip_date BETWEEN '2020-09-01' AND '2020-11-01' ORDER BY trip_num DESC LIMIT 10; 
-- found most of them are on Thusday/Friday: people tends to like going out or taking ubers at the end of weeks

-- Analysis 3
-- the lowest 10 number of trips after covid begins
SELECT trip_date, trip_num FROM date_trips ORDER BY trip_num LIMIT 10; 
-- One of them is 2021-02-01, let's see what happened? scarry blizard in NY, the trip is a big drop

-- Analysis 4
-- the highest 10 number of trips after covid begins
SELECT trip_date, trip_num FROM date_trips ORDER BY trip_num DESC LIMIT 10; 
-- all of them are before the first case of covid has been identified