import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplay;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events (
    id int IDENTITY(0, 1) PRIMARY KEY,
    artist varchar,
    auth varchar,
    first_name varchar,
    gender varchar,
    item_in_session int,
    last_name varchar,
    length float,
    level varchar,
    location varchar,
    method varchar,
    page varchar,
    registration float,
    session_id int,
    song varchar,
    status int,
    timestamp timestamp,
    user_agent varchar,
    user_id int)

    DISTKEY (page)

;
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs (
    artist_id varchar,
    artist_latitude float,
    artist_location varchar,
    artist_longitude float,
    artist_name varchar,
    duration float,
    num_songs int,
    song_id varchar,
    title varchar,
    year int)

    DISTSTYLE AUTO
;
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
    songplays_id int IDENTITY(0, 1) PRIMARY KEY,
    time_id int NOT NULL,
    user_id int NOT NULL,
    level varchar(4),
    song_id varchar(18) NOT NULL,
    artist_id varchar(18) NOT NULL,
    session_id int,
    location varchar,
    user_agent varchar
)
    DISTSTYLE AUTO
; 
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
    user_id int PRIMARY KEY,
    first_name varchar,
    last_name varchar,
    gender varchar,
    level varchar(4)
)
    DISTSTYLE AUTO
;
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
    song_id varchar(18) PRIMARY KEY,
    title varchar NOT NULL,
    artist_id varchar(18) NOT NULL,
    year int,
    duration float
)
    DISTSTYLE AUTO;
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
    artist_id varchar(18) PRIMARY KEY,
    name varchar NOT NULL,
    location varchar,
    latitude float,
    longitude float
)
    DISTSTYLE AUTO;
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
    id int IDENTITY(0, 1) PRIMARY KEY,
    ts timestamp UNIQUE,
    hour int,
    day int,
    week int,
    month int,
    weekday varchar
)
    DISTSTYLE AUTO;
""")

# STAGING TABLES

staging_events_copy = ("""
COPY staging_events FROM '{}'
credentials 'aws_iam_role={}'
IGNOREHEADER AS 1
NULL AS 'null'
TIMEFORMAT AS 'epochmillisecs'
gzip delimiter '|' compupdate off region '{}';
""").format(
    config['S3']['LOG_DATA_OUT'],
    config['IAM_ROLE']['ARN'],
    config['REGION']['AWS_REGION']
)

staging_songs_copy = ("""
COPY staging_songs FROM '{}'
credentials 'aws_iam_role={}'
IGNOREHEADER AS 1
NULL AS 'null'
REMOVEQUOTES
gzip delimiter '|' compupdate off region '{}';
""").format(
    config['S3']['SONG_DATA_OUT'],
    config['IAM_ROLE']['ARN'],
    config['REGION']['AWS_REGION']
)

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (

    time_id,
    user_id,
    level,
    song_id,
    artist_id,
    session_id,
    location,
    user_agent
)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s)

""")

user_table_insert = ("""
INSERT INTO users (
    user_id,
    first_name,
    last_name,
    gender,
    level
)
VALUES (%s, %s, %s, %s, %s)
""")

song_table_insert = ("""
INSERT INTO songs (
    song_id,
    artist_id,
    title,
    year,
    duration
)
VALUES (%s, %s, %s, %s, %s)
""")

artist_table_insert = ("""
INSERT INTO artists (
    artist_id,
    name,
    location,
    latitude,
    longitude
)
VALUES (%s, %s, %s, %s, %s)
""")

time_table_insert = ("""
INSERT INTO time (
    ts,
    hour,
    day,
    week,
    month,
    weekday
)
VALUES (%s, %s, %s, %s, %s, %s)
""")

# SELECT STATEMENTS

songplay_table_select = ("""
SELECT 
    t.id AS time_id,
    se.user_id,
    se.level,
    s.song_id,
    a.artist_id,
    se.session_id,
    se.location,
    se.user_agent 

FROM staging_events se

LEFT JOIN time t
ON se.timestamp = t.ts

LEFT JOIN songs s
ON se.song = s.title

LEFT JOIN artists a
ON se.artist = a.name

WHERE se.page = 'NextSong'
AND t.id IS NOT NULL 
AND se.user_id IS NOT NULL 
AND s.song_id IS NOT NULL 
AND a.artist_id IS NOT NULL 

""")

user_table_select = ("""
SELECT DISTINCT user_id,
        first_name,
        last_name,
        gender,
        level
FROM staging_events se
WHERE user_id IS NOT NULL
AND se.page = 'NextSong'
""")

time_table_select = ("""
SELECT DISTINCT timestamp AS ts
        FROM staging_events se
        WHERE ts IS NOT NULL
        AND se.page = 'NextSong'
""")

artist_table_select = ("""
SELECT DISTINCT artist_id,
        artist_name,
        artist_location,
        artist_latitude,
        artist_longitude
FROM staging_songs

WHERE artist_id IS NOT NULL
AND artist_name IS NOT NULL
""")

song_table_select = ("""
SELECT  DISTINCT song_id,
        artist_id,
        title,
        year,
        duration
FROM staging_songs

WHERE song_id IS NOT NULL
AND artist_id IS NOT NULL
AND title IS NOT NULL
""")


# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]

song_user_artist_songplay_table_insert = [
    (user_table_select, user_table_insert),
    (song_table_select, song_table_insert),
    (artist_table_select, artist_table_insert),
    (songplay_table_select, songplay_table_insert)
    ]

time_table_insert_query = (time_table_select, time_table_insert)
