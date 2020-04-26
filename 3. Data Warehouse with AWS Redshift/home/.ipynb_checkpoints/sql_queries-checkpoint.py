import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
LOG_DATA = config.get("S3","LOG_DATA")
LOG_JSONPATH = config.get("S3","LOG_JSONPATH")
SONG_DATA = config.get("S3","SONG_DATA")
ARN = config.get("IAM_ROLE","ARN")
REGION = config.get("CLUSTER","REGION")

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"


# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS 
staging_events 
(
    artist varchar,
    auth varchar,
    first_name varchar,
    gender varchar,
    item_in_session integer,
    last_name varchar,
    length float,
    level varchar,
    location varchar,
    method varchar,
    page varchar,
    registration varchar,
    session_id integer,
    song varchar,
    status varchar,
    time_stamp bigint,
    user_agent varchar,
    user_id integer
)
diststyle auto
;
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS 
staging_songs 
(
    num_songs integer,
    artist_id varchar, 
    artist_latitude float,
    artist_longitude float,
    artist_location varchar,
    artist_name varchar,
    song_id varchar,
    title varchar,
    duration float,
    year integer
)
diststyle auto
;
""")

songplay_table_create = """ 
CREATE TABLE IF NOT EXISTS 
songplays 
(
songplay_id integer identity(0,1) not null, 
start_time timestamp not null, 
user_id integer not null, 
level varchar not null, 
song_id varchar, 
artist_id varchar, 
session_id varchar not null, 
location varchar, 
user_agent varchar,
primary key(songplay_id)
)
distkey(song_id)
sortkey(start_time)
;
"""

user_table_create = """
CREATE TABLE IF NOT EXISTS 
users 
(
user_id integer not null, 
first_name varchar, 
last_name varchar, 
gender varchar, 
level varchar, 
primary key(user_id)
)
diststyle even
;
"""

song_table_create = """
CREATE TABLE IF NOT EXISTS 
songs
(
song_id varchar not null, 
title varchar, 
artist_id varchar, 
year integer, 
duration float,
primary key(song_id)
)
diststyle key
distkey (artist_id)
;
"""

artist_table_create = """
CREATE TABLE IF NOT EXISTS 
artists 
(
artist_id varchar not null, 
name varchar, 
location varchar, 
latitude float, 
longitude float,
primary key(artist_id)
);
"""

time_table_create = """
CREATE TABLE IF NOT EXISTS 
time 
(
start_time timestamp, 
hour integer, 
day integer, 
week integer, 
month integer, 
year integer, 
weekday integer,
primary key(start_time)
);
"""

# STAGING TABLES

staging_events_copy = ("""
COPY staging_events FROM {}
CREDENTIALS 'aws_iam_role={}'
JSON {}
REGION '{}';
""").format(LOG_DATA, ARN, LOG_JSONPATH, REGION)

staging_songs_copy = ("""
COPY staging_songs from {}
CREDENTIALS 'aws_iam_role={}'
JSON 'auto'
REGION '{}';
""").format(SONG_DATA, ARN, REGION)

# FINAL TABLES

user_table_insert = ("""
INSERT INTO users 
(
user_id, first_name, last_name, gender, level
) 
SELECT user_id, first_name, last_name, gender, max(level) as level
from staging_events
where user_id is not NULL
group by user_id, first_name, last_name, gender
;
""")

song_table_insert = ("""
INSERT INTO songs 
(
song_id, title, artist_id, year, duration
)
SELECT DISTINCT song_id, title, artist_id, year, duration
FROM staging_songs
;
""")

artist_table_insert = ("""
INSERT INTO artists 
(
artist_id, name, location, latitude, longitude
)
SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
FROM staging_songs
;
""")

#  SELECT DISTINCT time_stamp :: TIMESTAMP as start_time
time_table_insert = ("""
INSERT INTO time 
(
start_time, hour, day, week, month, year, weekday
)
SELECT start_time,
EXTRACT('hour' from start_time) as hour,
EXTRACT('day' from start_time) as day,
EXTRACT('week' from start_time) as week,
EXTRACT('month' from start_time) as month,
EXTRACT('year' from start_time) as year,
EXTRACT('weekday' from start_time) as weekday
FROM 
(
  SELECT DISTINCT TIMESTAMP 'epoch' + time_stamp/1000 * interval '1 second' AS start_time
  FROM staging_events
);
""")

songplay_table_insert = ("""
INSERT INTO songplays 
(
start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
)
SELECT TIMESTAMP 'epoch' + time_stamp/1000 * interval '1 second' AS start_time, 
user_id, se.level, sa.song_id, sa.artist_id, se.session_id, se.location, se.user_Agent
FROM staging_events se
LEFT JOIN 
(
    SELECT s.song_id, s.title, s.duration, a.artist_id, a.name
    FROM songs s
    JOIN artists a
    ON s.artist_id = a.artist_id
) sa
ON    se.song = sa.title
AND   se.length = sa.duration
AND   se.artist = sa.name
WHERE se.page = 'NextSong'
;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [user_table_insert, song_table_insert, artist_table_insert, time_table_insert, songplay_table_insert]

# TABLE LIST

table_list = ['staging_events', 'staging_songs', 'users', 'songs', 'artists', 'time', 'songplays']
