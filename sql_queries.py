import configparser


# CONFIG
config = configparser.ConfigParser()
config.read_file(open('dwh.cfg'))
ARN = config.get("IAM_ROLE", "ARN")
LOG_DATA = config.get("S3", "LOG_DATA")
LOG_JSONPATH = config.get("S3", "LOG_JSONPATH")
SONG_DATA = config.get("S3", "SONG_DATA")

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events(
        artist             TEXT,
        auth               VARCHAR(10),
        firstName          VARCHAR(15),
        gender             VARCHAR(1),
        itemInSession      INTEGER,
        lastName           VARCHAR(15),
        length             NUMERIC,
        level              VARCHAR(5),
        location           TEXT,
        method             VARCHAR(5),
        page               VARCHAR(20),
        registration       INT8,
        sessionId          INTEGER,
        song               TEXT,
        status             INTEGER,
        ts                 TIMESTAMP,
        userAgent          TEXT,
        userId             INTEGER);
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs(
        num_songs          INTEGER,
        artist_id          TEXT,
        artist_latitude    FLOAT,
        artist_longitude   FLOAT,
        artist_location    TEXT,
        artist_name        TEXT,
        song_id            TEXT,
        title              TEXT,
        duration           NUMERIC,
        year               INTEGER);
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays(
        songplay_id INTEGER IDENTITY(0, 1) PRIMARY KEY NOT NULL, 
        start_time  TIMESTAMP sortkey, 
        user_id     INTEGER, 
        level       VARCHAR(5), 
        song_id     TEXT, 
        artist_id   TEXT, 
        session_id  INTEGER, 
        location    TEXT, 
        user_agent  TEXT);
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users(
        user_id    INTEGER PRIMARY KEY NOT NULL sortkey, 
        first_name VARCHAR(15) NOT NULL, 
        last_name  VARCHAR(15), 
        gender     VARCHAR(1), 
        level      VARCHAR(5));
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs(
        song_id   TEXT PRIMARY KEY NOT NULL sortkey, 
        title     TEXT, 
        artist_id TEXT, 
        year      INTEGER, 
        duration  NUMERIC);
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists(
        artist_id TEXT PRIMARY KEY NOT NULL sortkey, 
        name      TEXT, 
        location  TEXT, 
        latitude  FLOAT, 
        longitude FLOAT);
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time(
        start_time TIMESTAMP PRIMARY KEY NOT NULL sortkey,
        hour       INTEGER, 
        day        INTEGER, 
        week       INTEGER, 
        month      INTEGER, 
        year       INTEGER, 
        weekday    INTEGER);
""")

# STAGING TABLES

staging_events_copy = ("""
COPY staging_events 
FROM {}
CREDENTIALS 'aws_iam_role={}' 
JSON {}
REGION 'us-west-2'
timeformat 'epochmillisecs';
""").format(LOG_DATA, ARN, LOG_JSONPATH)

staging_songs_copy = ("""
COPY staging_songs
FROM {} 
CREDENTIALS 'aws_iam_role={}' 
REGION 'us-west-2'
JSON 'auto';
""").format(SONG_DATA, ARN)

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT DISTINCT(e.ts) AS start_time,
       e.userId       AS user_id,
       e.level        AS level,
       s.song_id      AS song_id,
       s.artist_id    AS artist_id,
       e.sessionId    AS session_id,
       e.location     AS location,
       e.userAgent    AS user_agent
FROM staging_events e
JOIN staging_songs s ON e.artist = s.artist_name AND e.song = s.title
WHERE e.page = 'NextSong';
""")

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
SELECT DISTINCT(userId) AS user_id,
       firstName        AS first_name,
       lastName         AS last_name,
       gender,
       level
FROM staging_events
WHERE userId IS NOT NULL;
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration)
SELECT DISTINCT(song_id) AS song_id,
       title,
       artist_id,
       year,
       duration
FROM staging_songs
WHERE song_id IS NOT NULL;
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude)
SELECT DISTINCT(artist_id) AS artist_id,
       artist_name         AS name,
       artist_location     AS location,
       artist_latitude     AS latitude,
       artist_longitude    AS longitude
FROM staging_songs
WHERE artist_id IS NOT NULL;
""")

time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
SELECT DISTINCT(ts)               AS start_time,
       extract(hour from ts)      AS hour,
       extract(day from ts)       AS day,
       extract(week from ts)      AS week,
       extract(month from ts)     AS month,
       extract(year from ts)      AS year,
       extract(dayofweek from ts) AS weekday
FROM staging_events
WHERE ts IS NOT NULL;
""")

# TEST TABLES

staging_events_table_test = ("""
SELECT count(*) 
FROM staging_events
""")

staging_songs_table_test = ("""
SELECT count(*) 
FROM staging_songs
""")

songplay_table_test = ("""
SELECT count(*) 
FROM songplays
""")

user_table_test = ("""
SELECT count(*) 
FROM users
""")

song_table_test = ("""
SELECT count(*) 
FROM songs
""")

artist_table_test = ("""
SELECT count(*) 
FROM artists
""")

time_table_test = ("""
SELECT count(*) 
FROM time
""")


# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
test_table_queries = [staging_events_table_test, staging_songs_table_test, songplay_table_test, user_table_test, song_table_test, artist_table_test, time_table_test]