
# Purpose
This project is aiming to analyze the data that a startup called Sparkify has been collected on songs and user activity on their new music streaming app.
The main focus is to understand what songs users are listening to. We are using Redshift as the storage, due to it is on cloud, so it is safe, trustable, scalable, and have good performance.

# How to run the scripts

- ## Pre-condition
Need to create a Redshift cluster, and IAM role for redshift to read data from S3, and update the corresponding url/parameters in dwh.cfg

- ## Steps
1. Firstly run the python file: create_tables.py
2. Then run the python file: etl.py
3. Lastly run the file to test the result: test.ipynb

# Files in the repository
1. create_tables.py: To create tables in redshift.
2. dwh.cfg: To put common parameters that we need to connect to AWS.
3. etl.py: Copy data from S3 bucket to redshift, and splited into different tables.
4. sql_queries.py: To write the drop/create/select/copy sqls in 1 place.
5. test.ipynb: To test the overall result and see if all data has been inserted into the tables.

# Database design and ETL pipeline

- ## Database design
There are 5 tables in total, and we are using star schema for this project. 1 Fact table called songplay, and 4 dimension tables called users, songs, artists, time. 

- ## ETL pipeline
All data comes from 2 staging table: staging_events and staging_songs, which copy directly from S3. Then we insert the data from 2 staging tables into rthe real 5 tables we need to feed the star schema design.