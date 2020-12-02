# Data Modeling with Postgres: Song Play Analysis

A startup called **Sparkify** wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The **analytical goal** is to understande what songs users are listening to. Currently, they don't have an easy way to query their data, which resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app. This project creates a STAR-schema database in PostgresSQL with particularly 5 tables through ETL pipelines in Python.

### Schema
Fact Table(1) and Dimension Tables(4) are defined for analytics focus.

#### Fact Table
**songplays** - records in log data associated with song plays i.e. records with page NextSong
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

### Database Python Script
A python script for creating and recreating database is provided. Run the following in terminal.
```
python create_tables.py
```

### ETL Notebook
**ETL Implementation**
- A Jupiter notebook named as **etl.ipynp** is provided to document and demonstrate the entire process of ETL pipeline.  

**ETL Test**
- A Jupiter notebook named as **test.ipynp** is used for maksing sure that ETL code is working correctly.

### ETL Python Script
This ETL python script automatically loops through logs and songs directories, transforms the data using Python/Pandas, and inserts it on the star-schema with relationships, where appropriate. Run the following in terminal.
```
python etl.py
```

> Note: The data directories include just a sub-set of the files.
