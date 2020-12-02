# Data Modeling with Postgres: Song Play Analysis

A music streaming startup, Sparkify, has grown their user base and song database even more and want to *move their data warehouse to a data lake*. In this project, I'm going to **build an ETL pipeline to extract their data from S3**, **process the data into analytics tables using Spark**, and then **load the data back into S3 as a set of dimensional tables**. The Spark process will be deployed on a AWS Cluster.

### Datasets
There are two datasets that reside in public S3 buckets:
- Song data: ```s3://udacity-dend/song_data```
- Log data: ```s3://udacity-dend/log_data```
Both of song_data and log_data are in JSON format.

### Schema
A star-schema database is created and optimized for queries on song play analysis, and this includes the following tables:

#### Fact Table
**songplays** - records in log data associated with song plays i.e. records with page ```NextSong```
- *songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent*

#### Dimension Tables
**users** - users in the app
- *user_id, first_name, last_name, gender, level*

**songs** - songs in music database
- *song_id, title, artist_id, year, duration*

**artists** - artists in music database
- *artist_id, name, location, lattitude, longitude*

**time** - timestamps of records in songplays broken down into specific units
- *start_time, hour, day, week, month, year, weekday*

### Spark Process

The ETL process is mainly having two steps:

First, ```song_data``` is processed. A SparkSession object is created and used to read data from S3 bucket - songs, then those song related JSON files are iterated over selecting relevant information from artists and songs into parquet.

Second, ```log_data``` is processed. As above, the SparkSession object is reading data from S3 bucket - logs, those log JSON files are filtered by *NextSong*. Then, information like date, time, year etc are extracted to parquet files.