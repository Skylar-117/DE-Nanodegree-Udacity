import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events (
        event_id      BIGINT IDENTITY(0,1) NOT NULL,
        artist        VARCHAR,
        auth          VARCHAR,
        firstName     VARCHAR,
        gender        VARCHAR,
        itemInSession VARCHAR,
        lastName      VARCHAR,
        length        VARCHAR,
        level         VARCHAR,
        location      VARCHAR,
        method        VARCHAR,
        page          VARCHAR,
        registration  VARCHAR,
        sessionId     INTEGER NOT NULL SORTKEY DISTKEY,
        song          VARCHAR,
        status        INTEGER,
        ts            BIGINT NOT NULL,
        userAgent     VARCHAR,
        userId        INTEGER
    );
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs (
        num_songs        INT,
        artist_id        VARCHAR NOT NULL SORTKEY DISTKEY,
        artist_latitude  VARCHAR,
        artist_longitude VARCHAR,
        artist_location  VARCHAR,
        artist_name      VARCHAR,
        song_id          VARCHAR NOT NULL,
        title            VARCHAR,
        duration         DECIMAL,
        year             INT
    );
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
        songplay_id INT IDENTITY(0,1) NOT NULL SORTKEY PRIMARY KEY,
        start_time TIMESTAMP NOT NULL,
        user_id    VARCHAR NOT NULL DISTKEY,
        level      VARCHAR NOT NULL,
        song_id    VARCHAR NOT NULL,
        artist_id  VARCHAR NOT NULL,
        session_id VARCHAR NOT NULL,
        location   VARCHAR,
        user_agent VARCHAR
    );
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
        user_id    INT NOT NULL SORTKEY PRIMARY KEY,
        first_name VARCHAR,
        last_name  VARCHAR,
        gender     VARCHAR,
        level      VARCHAR
    );
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
        song_id   VARCHAR NOT NULL SORTKEY PRIMARY KEY,
        title     VARCHAR NOT NULL,
        artist_id VARCHAR NOT NULL,
        year      INT NOT NULL,
        duration  DECIMAL NOT NULL
    );
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
        artist_id VARCHAR NOT NULL SORTKEY PRIMARY KEY,
        name      VARCHAR,
        location  VARCHAR,
        latitude  DECIMAL,
        longitude DECIMAL
    );
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
        start_time TIMESTAMP NOT NULL SORTKEY PRIMARY KEY,
        hour       SMALLINT,
        day        SMALLINT,
        week       SMALLINT,
        month      SMALLINT,
        year       SMALLINT,
        weekday    SMALLINT
    );
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events
    FROM {}
    CREDENTIALS 'aws_iam_role={}'
    FORMAT AS json {}
""").format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'], config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
    COPY staging_songs
    FROM {}
    CREDENTIALS 'aws_iam_role={}'
    FORMAT AS json 'auto'
""").format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT DISTINCT to_timestamp(to_char(se.ts, '9999-99-99 99:99:99'),'YYYY-MM-DD HH24:MI:SS'),
           se.userId    AS user_id,
           se.level     AS level,
           ss.song_id   AS song_id,
           ss.artist_id AS artist_id,
           se.sessionId AS session_id,
           se.location  AS location,
           se.userAgent AS user_agent
    FROM staging_events se
    JOIN staging_songs ss ON se.song = ss.title AND se.artist = ss.artist_name
    WHERE se.page='NextSong';
""")

user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT DISTINCT userId AS user_id,
           firstName       AS first_name,
           lastName        AS last_name,
           gender          AS gender,
           level           AS level
    FROM staging_events
    WHERE userId IS NOT NULL AND page='NextSong';
""")

song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT  DISTINCT ss.song_id AS song_id,
            ss.title            AS title,
            ss.artist_id        AS artist_id,
            ss.year             AS year,
            ss.duration         AS duration
    FROM staging_songs AS ss
    WHERE song_id IS NOT NULL;
""")

artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, location, latitude, longitude)
    SELECT DISTINCT ss.artist_id AS artist_id,
           ss.artist_name        AS name,
           ss.artist_location    AS location,
           ss.artist_latitude    AS latitude,
           ss.artist_longitude   AS longitude
    FROM staging_songs AS ss
    WHERE artist_id IS NOT NULL;
""")

time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT DISTINCT TIMESTAMP 'epoch' + se.ts/1000 \
                * INTERVAL '1 second'        AS start_time,
            EXTRACT(hour FROM start_time)    AS hour,
            EXTRACT(day FROM start_time)     AS day,
            EXTRACT(week FROM start_time)    AS week,
            EXTRACT(month FROM start_time)   AS month,
            EXTRACT(year FROM start_time)    AS year,
            EXTRACT(week FROM start_time)    AS weekday
    FROM staging_events se
    WHERE ts IS NOT NULL AND se.page='NextSong';
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]

drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]

copy_table_queries = [staging_events_copy, staging_songs_copy]

insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]