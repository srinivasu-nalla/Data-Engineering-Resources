// STEP 1 : Create 3 Tables for tblAlubum, tblArtist, tblSongs

// Create Seperate Database and schema

CREATE OR REPLACE DATABASE ETL_PROJECT;
USE DATABASE ETL_PROJECT;
CREATE OR REPLACE SCHEMA SPOTIFY;


CREATE OR REPLACE TABLE ETL_PROJECT.SPOTIFY.tblAlubum (
    album_id VARCHAR(100),
    name VARCHAR(100),
    release_date DATE,
    total_tracks INT,
    url VARCHAR(200));

CREATE OR REPLACE TABLE ETL_PROJECT.SPOTIFY.tblArtist (
    artist_id VARCHAR(100),
    artist_name VARCHAR(100),
    external_url VARCHAR(200));

CREATE OR REPLACE TABLE ETL_PROJECT.SPOTIFY.tblSongs (
    song_id VARCHAR(100),
    song_name VARCHAR(100),
    duration_ms INT,
    url VARCHAR(200),
    popularity INT,
    song_added TIMESTAMP,
    album_id VARCHAR(100),
    artist_id VARCHAR(100));

// STEP 2 : Create Storage Stage to test connection with AWS

// Create a Storage Integration &  Update IAM Role's Trust Relationship:
create or replace storage integration s3_init
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = S3
  ENABLED = TRUE 
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::813417990701:role/snowflake-s3-connection'
  STORAGE_ALLOWED_LOCATIONS = ('s3://spotify-etl-project-srinalla/')
   COMMENT = 'Creating connection to S3' 

// Create a File Format
CREATE OR REPLACE SCHEMA ETL_PROJECT.file_formats;
CREATE OR REPLACE file format ETL_PROJECT.file_formats.csv_fileformat
    type = csv
    field_delimiter = ','
    skip_header = 1
    null_if = ('NULL','null')
    empty_field_as_null = TRUE;

// Create an External Stage
CREATE OR REPLACE SCHEMA ETL_PROJECT.external_stages;
CREATE OR REPLACE stage ETL_PROJECT.external_stages.csv_folder
    URL = 's3://spotify-etl-project-srinalla/'
    STORAGE_INTEGRATION = s3_init
    FILE_FORMAT = ETL_PROJECT.file_formats.csv_fileformat

LIST @ETL_PROJECT.external_stages.csv_folder

// STEP 3 :  Test COPY COMMAND AND LOAD DATA

// For tblAlubum
COPY INTO ETL_PROJECT.SPOTIFY.tblAlubum
    FROM @ETL_PROJECT.external_stages.csv_folder
    PATTERN = '.*album_transformed.*.csv';

select count(*) from ETL_PROJECT.SPOTIFY.tblAlubum;
TRUNCATE ETL_PROJECT.SPOTIFY.tblAlubum;

// For tblArtist
COPY INTO ETL_PROJECT.SPOTIFY.tblArtist
    FROM @ETL_PROJECT.external_stages.csv_folder
    PATTERN = '.*artist_transformed.*.csv';

select count(*) from ETL_PROJECT.SPOTIFY.tblArtist;
TRUNCATE ETL_PROJECT.SPOTIFY.tblArtist;

// For tblSongs
COPY INTO ETL_PROJECT.SPOTIFY.tblSongs
    FROM @ETL_PROJECT.external_stages.csv_folder
    PATTERN = '.*song_transformed.*.csv';

select count(*) from ETL_PROJECT.SPOTIFY.tblSongs;
TRUNCATE ETL_PROJECT.SPOTIFY.tblSongs;

// STEP 4 : 
CREATE OR REPLACE SCHEMA ETL_PROJECT.pipes;

// For tblAlubum

CREATE OR REPLACE pipe ETL_PROJECT.pipes.Alubum_pipe
auto_ingest = TRUE
AS
COPY INTO ETL_PROJECT.SPOTIFY.tblAlubum
FROM @ETL_PROJECT.external_stages.csv_folder
PATTERN = '.*album_transformed.*.csv';

// For tblArtist
CREATE OR REPLACE pipe ETL_PROJECT.pipes.Artist_pipe
auto_ingest = TRUE
AS
COPY INTO ETL_PROJECT.SPOTIFY.tblArtist
    FROM @ETL_PROJECT.external_stages.csv_folder
    PATTERN = '.*artist_transformed.*.csv';

// For tblSongs
CREATE OR REPLACE pipe ETL_PROJECT.pipes.Songs_pipe
auto_ingest = TRUE
AS
COPY INTO ETL_PROJECT.SPOTIFY.tblSongs
    FROM @ETL_PROJECT.external_stages.csv_folder
    PATTERN = '.*song_transformed.*.csv';

SHOW PIPES;

// Retrieve SQS ARN ,copy the value from the notification_channel column 
// Create  S3 Event Notifications and update respective SQS Queue from above output 
SHOW PIPES like '%Alubum%' ; 
SHOW PIPES like '%Artist%' ; 
SHOW PIPES like '%Songs%' ; 

// Run and Test the Pipeline and validate the Table count
select count(*) from ETL_PROJECT.SPOTIFY.tblAlubum;
select count(*) from ETL_PROJECT.SPOTIFY.tblArtist;
select count(*) from ETL_PROJECT.SPOTIFY.tblSongs;



