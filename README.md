<!-- Connecting to postgres -->
create postgres DB table:
    because postgres.Queries need url, add plugin Defaults to set connection
    create a staging table to have unique id, and file name
Load data into table
Add unique id: specify unique id instead of randomly generating it.
Truncate staging table
Get rid of the outputs
Manage schedual, add trigger, backfill
    avoid multiple flows:
        1. create multiple staging table, then drop it after done
        2. only set only executing 1 flow

<!-- Connecting to GCP -->
Create a new project in GCP
Set an service account for it, be careful of permission, create eys
Set GCP credentials by adding key in kestra 
Create bucket and dataset in GCP by kestra
Upload csv to GCP
Create table in Bigquery
Create staging table in Bigquery
Add unique row id and file name
Merge data into main table




2022 
<!-- What is data lake -->
Raw, Large, Undefined
a. ingest structured and unstructured data
b. stores, secures and protects data at unlimited scale
c. catalogs and indexes for analysis without data movement
d. connects data with analytics and machine learning tools

<!-- What is data warehouse -->
Refined, Smaller, Relational

<!-- ETL vs ELT -->
a. ETL  ---> datawarehouse solution
   ELT  ---> data lake solution, based on schema on read

<!-- Gotcha of Data Lake -->
a. converting into Data Swamp
b. no versioning 
c. incompatible schemas for same data without versioning
d. no metadata associated
e. joins not possible

Products: 
GCP --> Cloud storage,   AWS --> S3, Azure --> AZURE BLOB