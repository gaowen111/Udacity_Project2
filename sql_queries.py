import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

#Get parameters from dwh.cfg file
HOST=config.get('CLUSTER','HOST')
DB_NAME=config.get('CLUSTER','DB_NAME')
DB_USER=config.get('CLUSTER','DB_USER')
DB_PASSWORD=config.get('CLUSTER','DB_PASSWORD')
DB_PORT=config.get('CLUSTER','DB_PORT')

DWH_ROLE_ARN=config.get('IAM_ROLE','ARN')

LOG_DATA=config.get('S3','LOG_DATA')
LOG_JSONPATH=config.get('S3','LOG_JSONPATH')
SONG_DATA=config.get('S3','SONG_DATA')

# DROP TABLES

staging_events_table_drop = "Drop table if exists staging_events"
staging_songs_table_drop = "Drop table if exists staging_songs"
songplay_table_drop = "Drop table if exists songplay"
user_table_drop = "Drop table if exists users"
song_table_drop = "Drop table if exists songs"
artist_table_drop = "Drop table if exists artists"
time_table_drop = "Drop table if exists time"

# CREATE TABLES
#If copy failed, then remove event_id here
staging_events_table_create= ("""
CREATE TABLE staging_events 
(
  artist        VARCHAR(255) NULL,
  auth          VARCHAR(255) NULL,
  firstName     VARCHAR(255) NULL,
  gender        VARCHAR(10) NULL,
  itemInSession INTEGER NULL,
  lastName      VARCHAR(255) NULL,
  length        FLOAT NULL,
  level         VARCHAR(10) NULL,
  location      VARCHAR(255) NULL,
  method        VARCHAR(10) NULL,
  page          VARCHAR(30) NULL,
  registration  BIGINT NULL,
  sessionId     INTEGER NULL,
  song          VARCHAR(255) NULL,
  status        INTEGER NULL,
  ts            TIMESTAMP NULL,
  userAgent     VARCHAR(1024) NULL,
  userId        INTEGER NULL
);""")

staging_songs_table_create = ("""
CREATE TABLE staging_songs 
(
  num_songs        INTEGER NULL,
  artist_id        VARCHAR(255) NOT NULL,
  artist_latitude  float NULL,
  artist_longitude float NULL,
  artist_location  VARCHAR(255) NULL,
  artist_name      VARCHAR(255) NULL,
  song_id          VARCHAR(255) NOT NULL primary key,
  title            VARCHAR(255) NULL,
  duration         FLOAT NULL,
  year             INTEGER NULL
);""")

songplay_table_create = ("""
CREATE TABLE songplay 
(
  songplay_id   bigint identity(0, 1) primary key,
  start_time    TIME NULL,
  user_id       INTEGER NULL,
  level         VARCHAR(10) NULL,
  song_id       VARCHAR(255) NULL,
  artist_id     VARCHAR(255) NULL,
  session_Id     INTEGER NULL,
  location      VARCHAR(255) NULL,
  user_agent     VARCHAR(1024) NULL
);""")

user_table_create = ("""
CREATE TABLE users 
(
  user_id       INTEGER NOT NULL primary key,
  first_name    VARCHAR(255) NULL,
  last_name     VARCHAR(255) NULL,
  gender        VARCHAR(10) NULL,
  level         VARCHAR(10) NULL
) diststyle all;""")

song_table_create = ("""
CREATE TABLE songs 
(
  song_id       VARCHAR(255) NOT NULL primary key,
  title         VARCHAR(255) NULL,
  artist_id     VARCHAR(255) NULL,
  year          INTEGER NULL,
  duration      FLOAT NULL
);""")

artist_table_create = ("""
CREATE TABLE artists 
(
  artist_id        VARCHAR(255) NOT NULL primary key,
  artist_name      VARCHAR(255) NULL,
  artist_location  VARCHAR(255) NULL,
  artist_latitude  FLOAT NULL,
  artist_longitude FLOAT NULL
);""")

time_table_create = ("""CREATE TABLE time 
(
  start_time  TIME NOT NULL primary key,
  hour        INTEGER NULL,
  day         INTEGER NULL,
  week        INTEGER NULL,
  month       INTEGER NULL,
  year        INTEGER NULL,
  weekday     INTEGER NULL
);""")

# STAGING TABLES
staging_events_copy = """
    copy staging_events from {}
    credentials 'aws_iam_role={}'
    format as json 's3://udacity-dend/log_json_path.json' 
    TIMEFORMAT as 'epochmillisecs'
    TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
    region 'us-west-2';
""".format(LOG_DATA,DWH_ROLE_ARN)

staging_songs_copy = """
    copy staging_songs from {}
    credentials 'aws_iam_role={}'
    json 'auto'
    region 'us-west-2';
""".format(SONG_DATA,DWH_ROLE_ARN)

# FINAL TABLES

songplay_table_insert = ("""Insert into songplay (start_time, user_id, level, song_id, artist_id,  session_Id, location, user_agent) select DISTINCT a.ts, a.userId, a.level, b.song_id, b.artist_id, a.sessionId, b.artist_location, a.userAgent from staging_events a left join staging_songs b on a.song = b.title and a.artist = b.artist_name and page = 'NextSong';
""")

user_table_insert = ("""Insert into users select DISTINCT userId, firstName, lastName, gender, level from staging_events where page = 'NextSong' ;
""")

song_table_insert = ("""Insert into songs select DISTINCT song_id, title, artist_id, year, duration from staging_songs;
""")

artist_table_insert = ("""Insert into artists select DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude from staging_songs;
""")

time_table_insert = ("""Insert into time select DISTINCT ts, EXTRACT(HOUR FROM ts), EXTRACT(DAY FROM ts), EXTRACT(WEEK FROM ts), EXTRACT(MONTH FROM ts), EXTRACT(YEAR FROM ts), EXTRACT(WEEKDAY FROM ts) from songplay;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
