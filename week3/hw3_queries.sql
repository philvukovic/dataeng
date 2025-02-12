CREATE OR REPLACE EXTERNAL TABLE `data-pipelines-450717.taxi_rides_ny_yellow_2024.yellow_taxi_raw`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://y25-q1-data-pipeline/yellow_tripdata_2024-*.parquet']
);


SELECT count(*) FROM `data-pipelines-450717.taxi_rides_ny_yellow_2024.yellow_taxi_raw`;

CREATE OR REPLACE TABLE `data-pipelines-450717.taxi_rides_ny_yellow_2024.yellow_taxi_raw_nonpartitioned`
AS SELECT * FROM `data-pipelines-450717.taxi_rides_ny_yellow_2024.yellow_taxi_raw`;

SELECT COUNT(DISTINCT(PULocationID)) FROM `data-pipelines-450717.taxi_rides_ny_yellow_2024.yellow_taxi_raw`;
SELECT COUNT(DISTINCT(PULocationID)) FROM `data-pipelines-450717.taxi_rides_ny_yellow_2024.yellow_taxi_raw_nonpartitioned`;

SELECT fare_amount, COUNT(1)
FROM `data-pipelines-450717.taxi_rides_ny_yellow_2024.yellow_taxi_raw_nonpartitioned`
WHERE fare_amount = 0.0
GROUP BY fare_amount;

CREATE OR REPLACE TABLE `data-pipelines-450717.taxi_rides_ny_yellow_2024.yellow_taxi_raw_partitioned`
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID AS (
  SELECT * FROM `data-pipelines-450717.taxi_rides_ny_yellow_2024.yellow_taxi_raw`
);

-- q6: partitioned
SELECT VendorID FROM `data-pipelines-450717.taxi_rides_ny_yellow_2024.yellow_taxi_raw_partitioned`
WHERE DATE(tpep_dropoff_datetime) BETWEEN '2024-03-01' AND '2024-03-15'
GROUP BY VendorID;

-- q6: non-partitioned
SELECT VendorID FROM `data-pipelines-450717.taxi_rides_ny_yellow_2024.yellow_taxi_raw_nonpartitioned`
WHERE DATE(tpep_dropoff_datetime) BETWEEN '2024-03-01' AND '2024-03-15'
GROUP BY VendorID;