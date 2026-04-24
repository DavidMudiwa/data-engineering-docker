CREATE EXTERNAL TABLE `kestra-sandbox-492709.data_warehouse.yellow_trips`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://dezoomcamp_hw3-david-492709/yellow_tripdata_2024-*.parquet'],
  enable_list_inference = true
);