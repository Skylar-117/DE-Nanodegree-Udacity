# Cloud Data Warehouse with AWS Redshift
This project is going to build the cloud-based data warehouse on AWS Redshift with two AWS services be used - ``S3``(Storage) and ``Redshift``(Database). The ETL(Extract, Transform, Load) pipeline is built to create database and tables in Redshift Database, fetch data from JSON files stored in S3, process data and insert to Redshift Database.

### Data Sources:
Data is a bunch of JSON formatted files stored in public AWS S3 buckets``s3://udacity-dend``, there are two parts of the data:
* **s3://udacity-dend/log_data**: information about artists and songs
* **s3://udacity-dend/song_data**: information about user actions, e.g., what songs is listened to etc...
* **[Additional] s3://udacity-dend/log_json_path.json**: this is an additional file representing the content and format for copying log data.

### Staging tables
The goal is to copy data content from JSON files (log_data and song_data) to staging tables. This is done by COPY command which allows access to JSON files in S3 buckets.

* **staging_events**: event data telling what users have done (columns: event_id, artist, auth, firstName, gender, itemInSession, lastName, length, level, location, method, page, registration, sessionId, song, status, ts, userAgent, userId)
* **staging_songs**: song data about songs and artists (columns: num_songs, artist_id, artist_latitude, artist_longitude, artist_location, artist_name, song_id, title, duration, year)

### Data Schema

- Fact table:
	- **songplays**: song play data together with user, artist, and song info (songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)

- Dimension tables
	- **users**: user info (columns: user_id, first_name, last_name, gender, level)
	- **songs**: song info (columns: song_id, title, artist_id, year, duration)
	- **artists**: artist info (columns: artist_id, name, location, latitude, longitude)
	- **time**: detailed time info about song plays (columns: start_time, hour, day, week, month, year, weekday)

### Project Steps:

- Create Table Schemas
	- Design schemas for fact and dimension tables
	- Write DROP/CREATE/INSERT INTO/SELECT statements for each of these tables in ``sql_queries.py``
	- Write drop_tables/create_tables functions and add docstrings for each of the function in ``create_tables.py``. 
	- Launch a redshift cluster and create an IAM role that has read access to S3.
	- Add redshift database and IAM role info to ``dwh.cfg``.
	- Test by running ``python create_tables.py`` and checking the table schemas by using Query Editor in AWS Redshift console for this.
	```
	SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES
	```

- Build ETL Pipeline
	- Write function `load_staging_tables` in ``etl.py`` to load data from S3 to staging tables on Redshift.
	- Write function `insert_tables` in ``etl.py`` to load data from staging tables to analytics tables on Redshift.
	- Test by running ``python etl.py`` after running ``python create_tables.py`` and running the analytic queries on your Redshift database to compare results with the expected results.
	- **Delete Redshift cluster when finished.**
Note that, ``etl.py`` first executes Redshift COPY command to insert source data (JSON files) to staging tables. Then from staging tables, data is further inserted to analytic tables.