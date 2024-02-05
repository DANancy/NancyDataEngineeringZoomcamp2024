-- Creating external table referring to green taxi trip records data for 2022
CREATE OR REPLACE EXTERNAL TABLE `dts-de-course-2024.ny_taxi.external_green_tripdata`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://trips_data_parquet/parquet/green/green_tripdata_2022-*.parquet']
);

-- Create a non partitioned table from external table
CREATE OR REPLACE TABLE dts-de-course-2024.ny_taxi.green_tripdata_non_partitoned AS
SELECT * FROM dts-de-course-2024.ny_taxi.external_green_tripdata;

-- Q1: What is count of records for the 2022 Green Taxi Data?
-- B: 840402
SELECT COUNT(*) FROM ny_taxi.external_green_tripdata;

-- Q2: Write a query to count the distinct number of PULocationIDs for the entire dataset on both the tables. What is the estimated amount of data that will be read when this query is executed on the External Table and the Table?
-- A?
SELECT COUNT(DISTINCT PULocationID) FROM dts-de-course-2024.ny_taxi.external_green_tripdata;
SELECT COUNT(DISTINCT PULocationID) FROM dts-de-course-2024.ny_taxi.green_tripdata_non_partitoned;


-- Q3: How many records have a fare_amount of 0?
-- D 1,622
SELECT COUNT(*) FROM ny_taxi.external_green_tripdata
WHERE FARE_AMOUNT = 0;

-- Q4: What is the best strategy to make an optimized table in Big Query if your query will always order the results by PUlocationID and filter based on lpep_pickup_datetime? (Create a new table with this strategy)
-- B
CREATE OR REPLACE TABLE `dts-de-course-2024.ny_taxi.green_partitioned_tripdata`
PARTITION BY DATE(lpep_pickup_datetime)
CLUSTER BY PUlocationID AS (
  SELECT * FROM `dts-de-course-2024.ny_taxi.external_green_tripdata`
);

-- Q5: Write a query to retrieve the distinct PULocationID between lpep_pickup_datetime 06/01/2022 and 06/30/2022 (inclusive). Use the materialized table you created earlier in your from clause and note the estimated bytes. Now change the table in the from clause to the partitioned table you created for question 4 and note the estimated bytes processed. What are these values?
-- B: 12.82 MB vs 1.12 MB 
SELECT COUNT(DISTINCT PULocationID) FROM ny_taxi.green_tripdata_non_partitoned
WHERE DATE(lpep_pickup_datetime) BETWEEN '2022-06-01' AND '2022-06-30';

SELECT COUNT(DISTINCT PULocationID) FROM ny_taxi.green_partitioned_tripdata
WHERE DATE(lpep_pickup_datetime) BETWEEN '2022-06-01' AND '2022-06-30'

-- Q6: Where is the data stored in the External Table you created?
-- GCP Bucket 

-- Q7: It is best practice in Big Query to always cluster your data
-- False

-- Q8: Write a SELECT count(*) query FROM the materialized table you created. How many bytes does it estimate will be read? Why?
-- 0
SELECT COUNT(*) FROM ny_taxi.green_partitioned_tripdata

