import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplay;"
user_table_drop = "DROP TABLE IF EXISTS user;"
song_table_drop = "DROP TABLE IF EXISTS song;"
artist_table_drop = "DROP TABLE IF EXISTS artist;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events (
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
    user_id int
);
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
    year int
);
""")

songplay_table_create = ("""
""")

user_table_create = ("""
""")

song_table_create = ("""
""")

artist_table_create = ("""
""")

time_table_create = ("""
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
""")

user_table_insert = ("""
""")

song_table_insert = ("""
""")

artist_table_insert = ("""
""")

time_table_insert = ("""
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create] #, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop] #, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
