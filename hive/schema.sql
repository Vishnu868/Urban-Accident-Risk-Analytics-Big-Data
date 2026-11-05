-- Hive tables for the urban mobility data lake

CREATE DATABASE IF NOT EXISTS urban_mobility;
USE urban_mobility;

CREATE EXTERNAL TABLE IF NOT EXISTS accidents_raw (
  id STRING, severity INT, start_time STRING, start_lat DOUBLE, start_lng DOUBLE,
  temperature_f DOUBLE, humidity DOUBLE, visibility_mi DOUBLE, precipitation_in DOUBLE,
  weather_condition STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/data/urban/accidents_raw'
TBLPROPERTIES ('skip.header.line.count'='1');

CREATE TABLE IF NOT EXISTS accidents_clean (
  hour INT, day_of_week INT, temperature_c DOUBLE, humidity DOUBLE, visibility_km DOUBLE,
  precipitation_mm DOUBLE, lat DOUBLE, lng DOUBLE, weather STRING, label INT
)
STORED AS PARQUET;

CREATE TABLE IF NOT EXISTS zone_risk_hourly (
  zone STRING, window_start TIMESTAMP, avg_risk DOUBLE, events BIGINT, alert STRING
)
PARTITIONED BY (dt STRING)
STORED AS PARQUET;

-- Top accident hotspots by grid cell
SELECT CONCAT(ROUND(lat, 2), '_', ROUND(lng, 2)) AS cell,
       COUNT(*) AS accidents, AVG(label) AS severe_rate
FROM accidents_clean
GROUP BY CONCAT(ROUND(lat, 2), '_', ROUND(lng, 2))
ORDER BY accidents DESC
LIMIT 20;
