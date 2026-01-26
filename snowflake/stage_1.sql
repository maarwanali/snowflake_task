USE WAREHOUSE MY_WH;

CREATE DATABASE AIRLINE_DB;
USE AIRLINE_DB;

CREATE SCHEMA BRONZE;
USE SCHEMA BRONZE;

-- CREATE a transient Table for loading raw data add data type in VARCHAR to avoid the Miss match data type and load all data to raw data table

CREATE OR REPLACE TRANSIENT TABLE airline_raw_data(
    id VARCHAR,
    passenger_id VARCHAR(50),
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    gender VARCHAR(10),
    age VARCHAR,
    nationality VARCHAR(50),
    airport_name VARCHAR(100),
    airport_country_code VARCHAR(10),
    country_name VARCHAR(50),
    airport_continent VARCHAR(50),
    continent VARCHAR(50),
    departure_date VARCHAR(50),
    arrival_airport VARCHAR(100),
    pilot_name VARCHAR(100),
    flight_status VARCHAR(50),
    ticket_type VARCHAR(50),
    passenger_status VARCHAR(50),
    _ingested_at          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
    );

--  CREATE EXTERNAL FILE & STORAGE & STAGE  to connect it with AWS s3 bucket
CREATE OR REPLACE FILE FORMAT csv_file_format
    TYPE = CSV
    FIELD_DELIMITER = ','
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    ESCAPE_UNENCLOSED_FIELD = NONE
    TRIM_SPACE = TRUE
    NULL_IF = ('', 'NULL')
    ERROR_ON_COLUMN_COUNT_MISMATCH = TRUE;


CREATE OR REPLACE STORAGE INTEGRATION s3_init_storage
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = S3
    ENABLED = TRUE
    STORAGE_AWS_ROLE_ARN='arn:aws:iam::838220667542:role/snow_aws_airline_acess'
    STORAGE_ALLOWED_LOCATIONS=('s3://airline-dataset-snowflake-pipe/');

DESC STORAGE INTEGRATION s3_init_storage;

    
CREATE OR REPLACE STAGE stg_raw_data
    URL='s3://airline-dataset-snowflake-pipe/'
    STORAGE_INTEGRATION=s3_init_storage
    FILE_FORMAT=csv_file_format;

LIST @stg_raw_data;


--  CREATE SNOWPIPE

SELECT SYSTEM$get_aws_sns_iam_policy('arn:aws:sns:eu-north-1:838220667542:airline_sns');


-- 1. Recreate the Pipe to refresh the metadata cache
CREATE OR REPLACE PIPE bronze_raw_pipe
    AUTO_INGEST = TRUE
    AWS_SNS_TOPIC='arn:aws:sns:eu-north-1:838220667542:airline_sns'
    AS 
    COPY INTO airline_raw_data (
        id, passenger_id, first_name, last_name, gender, age, 
        nationality, airport_name, airport_country_code, country_name, 
        airport_continent, continent, departure_date, arrival_airport, 
        pilot_name, flight_status, ticket_type, passenger_status
    )
    FROM (
        SELECT 
            s.$1, s.$2, s.$3, s.$4, s.$5, s.$6, s.$7, s.$8, s.$9, s.$10, 
            s.$11, s.$12, s.$13, s.$14, s.$15, s.$16, s.$17, s.$18
        FROM @stg_raw_data s
    )
    FILE_FORMAT = (FORMAT_NAME = 'csv_file_format')
    ON_ERROR = 'CONTINUE';


SHOW PIPES;  -- Get the notification_channel ARN for adding to sqs in s3 bucket


-- DROP PIPE bronze_raw_pipe;

-- SELECT * FROM TABLE(VALIDATE_PIPE_LOAD(
--    PIPE_NAME => 'bronze_raw_pipe', 
--    START_TIME => DATEADD(HOUR, -2, CURRENT_TIMESTAMP())
--));

-- SELECT * FROM airline_raw_data;



--  CREATE STREAM object to track airline_raw_data table (CDC)

CREATE OR REPLACE STREAM airline_raw_stream ON TABLE airline_raw_data;

ALTER PIPE AIRLINE_DB.BRONZE.bronze_raw_pipe REFRESH;

SELECT SYSTEM$STREAM_HAS_DATA('AIRLINE_DB.BRONZE.AIRLINE_RAW_STREAM')::BOOLEAN;


SHOW STREAMS LIKE 'AIRLINE_RAW_STREAM' IN SCHEMA AIRLINE_DB.BRONZE;

