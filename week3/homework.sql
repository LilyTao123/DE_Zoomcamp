--create external table
create or replace external table `yellow_taxi_2024.yellow_taxi_external`
OPTIONS (
  format = 'parquet',
  uris = ['gs://ny_taxi_lili/yellow_tripdata_2024-*.parquet']
);

-- create materialized table
create or replace table `yellow_taxi_2024.yellow_taxi`
as 
select * from yellow_taxi_2024.yellow_taxi_external;


--q1:   20,332,093
select count(1) from yellow_taxi_2024.yellow_taxi;

--q2: 0 MB for the External Table and 155.12 MB for the Materialized Table
select count(distinct PULocationID) from yellow_taxi_2024.yellow_taxi_external;

select count(distinct PULocationID) from yellow_taxi_2024.yellow_taxi;

--q3: 155.12
select PULocationID from yellow_taxi_2024.yellow_taxi;

--310.24
select PULocationID, DOLocationID from yellow_taxi_2024.yellow_taxi;
-- BigQuery duplicates data across multiple storage partitions, so selecting two columns instead of one requires scanning the table twice, doubling the estimated bytes processed.

--q4 8,333
select count(1) from yellow_taxi_2024.yellow_taxi
where fare_amount=0;

--q5 -- 2.7G
select * from yellow_taxi_2024.yellow_taxi
where date_trunc(tpep_dropoff_datetime, MONTH) = '2024-03-01'
order by VendorID;

create or replace table `yellow_taxi_2024.yellow_taxi_partitioned`
PARTITION BY DATE(tpep_dropoff_datetime) AS
select * from `yellow_taxi_2024.yellow_taxi`;

--488.11
select * from yellow_taxi_2024.yellow_taxi_partitioned
where date_trunc(tpep_dropoff_datetime, MONTH) = '2024-03-01'
order by VendorID;


create or replace table `yellow_taxi_2024.yellow_taxi_partitioned_cluster`
PARTITION BY DATE(tpep_dropoff_datetime) 
CLUSTER BY VendorID AS
select * from `yellow_taxi_2024.yellow_taxi`;

select * from yellow_taxi_2024.yellow_taxi_partitioned_cluster
where date_trunc(tpep_dropoff_datetime, MONTH) = '2024-03-01'
order by VendorID;


--q6 310.24 MB for non-partitioned table and 26.84 MB for the partitioned table
select distinct VendorID from yellow_taxi_2024.yellow_taxi
where date(tpep_dropoff_datetime) between '2024-03-01' and '2024-03-15';

select distinct VendorID from `yellow_taxi_2024.yellow_taxi_partitioned`
where date(tpep_dropoff_datetime) between '2024-03-01' and '2024-03-15';

--q7 GCP Bucket
--q8 no
--q9 0
SELECT count(1) from yellow_taxi_2024.yellow_taxi
