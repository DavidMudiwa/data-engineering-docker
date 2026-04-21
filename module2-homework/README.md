# Data Engineering Zoomcamp -- Module 2 Homework

This repository contains my answers to the\
[Module 2 Homework (Workflow Orchestration)](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/cohorts/2026/02-workflow-orchestration/homework.md)\
for **Data Engineering by DataTalksClub**.

------------------------------------------------------------------------

## Question 1


**Answer:** `364.7 MB`

------------------------------------------------------------------------

## Question 2


**Answer:** `green_tripdata_2020-04.csv`

------------------------------------------------------------------------

## Question 3

``` sql
SELECT SUM(row_count) AS total_rows
FROM `kestra-sandbox-492709.zoomcamp.__TABLES__`
WHERE table_id LIKE 'yellow_tripdata_2020_%'
  AND table_id NOT LIKE '%_ext';
```

**Answer:** `24,648,499`

------------------------------------------------------------------------

## Question 4

``` sql
SELECT SUM(row_count) AS total_rows
FROM `kestra-sandbox-492709.zoomcamp.__TABLES__`
WHERE table_id LIKE 'green_tripdata_2020_%'
  AND table_id NOT LIKE '%_ext';
```

**Answer:** `1,734,051`

------------------------------------------------------------------------

## Question 5

``` sql
SELECT COUNT(*) AS total_rows 
FROM `kestra-sandbox-492709.zoomcamp.yellow_tripdata_2021_03`
```

**Answer:** `1,925,152`

------------------------------------------------------------------------

## Question 6

**Answer:** Add a `timezone` property set to `America/New York` in the schedule trigger configuration.

