# Data Engineering Zoomcamp -- Module 3 Homework

This repository contains my answers to the\
[Module 3 Homework (Data Warehouse)](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/cohorts/2026/03-data-warehouse/homework.md)\
for **Data Engineering by DataTalksClub**.



## Question 1

``` sql
SELECT count(*) AS total_rows 
FROM `kestra-sandbox-492709.data_warehouse.yellow_trips`; 
```

**Answer:** `20,332,093`



## Question 2

``` sql
SELECT count(distinct PULocationID) as PU_count 
FROM `kestra-sandbox-492709.data_warehouse.yellow_trips`;
```
**Answer:** `0 MB for the External Table and 155.12 MB for the Materialized Table`



## Question 3

``` sql
SELECT PULocationID  
FROM `kestra-sandbox-492709.data_warehouse.yellow_trips`;

SELECT PULocationID,DOLocationID  
FROM `kestra-sandbox-492709.data_warehouse.yellow_trips`;

```

**Answer:** `BigQuery is a columnar database, and it only scans the specific columns requested in the query. Querying two columns (PULocationID, DOLocationID) requires reading more data than querying one column (PULocationID), leading to a higher estimated number of bytes processed.`



## Question 4

``` sql
SELECT COUNT(*) AS zero_fare_trips
FROM `kestra-sandbox-492709.data_warehouse.yellow_trips` WHERE fare_amount = 0;
```

**Answer:** `8,333`



## Question 5
``` sql
CREATE OR REPLACE TABLE `kestra-sandbox-492709.data_warehouse.yellow_trips_partitioned_clustered`
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID
AS
SELECT *
FROM `kestra-sandbox-492709.data_warehouse.yellow_trips`;
```
**Answer:** `Partition by tpep_dropoff_datetime and Cluster on VendorID`



## Question 6
``` sql
SELECT DISTINCT(VendorID)
FROM `kestra-sandbox-492709.data_warehouse.yellow_trips_partitioned_clustered`
WHERE tpep_dropoff_datetime>='2024-03-01' AND tpep_dropoff_datetime < '2024-03-16' ;

SELECT DISTINCT(VendorID)
FROM `kestra-sandbox-492709.data_warehouse.yellow_trips`
WHERE tpep_dropoff_datetime>='2024-03-01' AND tpep_dropoff_datetime < '2024-03-16' ;

```
**Answer:** `310.24 MB for non-partitioned table and 26.84 MB for the partitioned table` 



## Question 7

**Answer:** `GCP Bucket`



## Question 8

**Answer:** `True`



## Question 9

**Answer:** `2.72GB because it had to read every column in the dataset.`

