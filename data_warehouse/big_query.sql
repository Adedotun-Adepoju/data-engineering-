--creating external table referring to gcs path
CREATE OR REPLACE EXTERNAL TABLE `data-engineering-362120.trips_data_all.external_table`
OPTIONS (
    format = 'PARQUET',
    uris = ['gs://data_engineering_lake_data-engineering-362120/raw/2019-*.parquet', 'gs://data_engineering_lake_data-engineering-362120/raw/202-*.parquet']
)

--check trip data 
SELECT *
FROM data-engineering-362120.trips_data_all.external_table
LIMIT 10

-- create a non partitioned table from external table
CREATE OR REPLACE TABLE `data-engineering-362120.trips_data_all.external_table_non_partitioned` AS 
SELECT * FROM `data-engineering-362120.trips_data_all.external_table`

-- create a partitioned table from external table
CREATE OR REPLACE TABLE `data-engineering-362120.trips_data_all.external_table_partitioned` 
PARTITION BY 
    DATE(pickup_datetime) AS 
SELECT * FROM `data-engineering-362120.trips_data_all.external_table`

-- scanning 354 mb of data
SELECT DISTINCT(dispatching_base_num)
FROM `data-engineering-362120.trips_data_all.external_table_non_partitioned`
WHERE DATE(pickup_datetime) BETWEEN '2019-01-02' AND '2019-01-19'

--scanning 150 mb of data
SELECT DISTINCT(dispatching_base_num)
FROM `data-engineering-362120.trips_data_all.external_table_partitioned`
WHERE DATE(pickup_datetime) BETWEEN '2019-01-02' AND '2019-01-19'

-- look into the partitions
SELECT table_name, partition_id, total_rows
FROM trips_data_all.INFORMATION_SCHEMA.PARTITIONS 
WHERE table_name = 'external_table_partitioned'
ORDER BY total_rows DESC

-- creating a partition and cluster tavle
CREATE OR REPLACE TABLE `data-engineering-362120.trips_data_all.external_table_partitioned_clustered`
PARTITION BY DATE(pickup_datetime)
CLUSTER BY dispatching_base_num AS 
SELECT * FROM `data-engineering-362120.trips_data_all.external_table`