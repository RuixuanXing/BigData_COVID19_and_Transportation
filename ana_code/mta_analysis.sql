-- Analysis 1: Before COVID-19 (Mar 2020), which station has the most daily riders?
SELECT a.log_date, a.station, a.entries_count
FROM (SELECT log_date, MAX(entries_count) entries_count
FROM turnstile_group_by_station_remove_outliers
GROUP BY log_date) b JOIN turnstile_group_by_station_remove_outliers a ON a.log_date = b.log_date AND a.entries_count = b.entries_count
WHERE a.log_date < "2020-03-01";

-- Analysis 2: Before COVID-19 (Mar 2020), which station has the least daily riders?
SELECT a.log_date, a.station, a.entries_count
FROM (SELECT log_date, MIN(entries_count) entries_count
FROM turnstile_group_by_station_remove_outliers
GROUP BY log_date) b JOIN turnstile_group_by_station_remove_outliers a ON a.log_date = b.log_date AND a.entries_count = b.entries_count
WHERE a.log_date < "2020-03-01";

-- Analysis 3: Before COVID-19 (Mar 2020), on which day does GRAND ST station have the least riders?
SELECT * FROM turnstile_group_by_station_remove_outliers WHERE station = "GRAND ST" ORDER BY entries_count LIMIT 10;

-- Analysis 4: Before COVID-19 (Mar 2020), on which day does GRAND ST station have the most riders?
SELECT * FROM turnstile_group_by_station_remove_outliers WHERE station = "GRAND ST" ORDER BY entries_count DESC LIMIT 10;
