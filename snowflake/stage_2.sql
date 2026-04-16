USE DATABASE AIRLINE_DB;

CREATE SCHEMA SILVER;

-- create new table with type casting - Create a Stream to track the CDC
--  CREATE TABLE

CREATE OR REPLACE TABLE airline_silver(
    passenger_id STRING PRIMARY KEY,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    gender VARCHAR(10),
    age INT,
    nationality VARCHAR(50),
    airport_name VARCHAR(100),
    airport_country_code VARCHAR(10),
    country_name VARCHAR(50),
    airport_continent VARCHAR(50),
    continent VARCHAR(50),
    departure_date DATE,
    arrival_airport VARCHAR(100),
    pilot_name VARCHAR(100),
    flight_status VARCHAR(50),
    ticket_type VARCHAR(50),
    passenger_status VARCHAR(50),
    _last_updated          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);


--  /////////Temp Table for saveing data from stream - INCASE of Rolling back the data in stream will be lose ///////////////
CREATE OR REPLACE TABLE AIRLINE_DB.SILVER.airline_stream_buffer (
    passenger_id STRING,
    first_name STRING,
    last_name STRING,
    gender STRING,
    age STRING,
    nationality STRING,
    airport_name STRING,
    airport_country_code STRING,
    country_name STRING,
    airport_continent STRING,
    continent STRING,
    departure_date STRING,
    arrival_airport STRING,
    pilot_name STRING,
    flight_status STRING,
    ticket_type STRING,
    passenger_status STRING,

    METADATA$ACTION STRING,
    METADATA$ISUPDATE BOOLEAN,
    METADATA$ROW_ID STRING,
    
    _ingested_at TIMESTAMP_NTZ
);






--  Stored Procedure
CREATE OR REPLACE PROCEDURE AIRLINE_DB.SILVER.sp_load_airline_data_silver()
RETURNS STRING
LANGUAGE SQL
AS

DECLARE
    proc_name       STRING  DEFAULT 'sp_load_airline_data_silver';
    rows_affected   INTEGER DEFAULT 0;
    current_user    STRING  DEFAULT CURRENT_USER();
BEGIN
    -- Check if stream has data
   IF (
        NOT SYSTEM$STREAM_HAS_DATA('AIRLINE_DB.BRONZE.airline_raw_stream') 
    ) THEN
        RETURN 'NO DATA | Stream is empty';
    END IF;
    
    BEGIN TRANSACTION;

        INSERT INTO AIRLINE_DB.SILVER.airline_stream_buffer (
        passenger_id,
        first_name,
        last_name,
        gender,
        age,
        nationality,
        airport_name,
        airport_country_code,
        country_name,
        airport_continent,
        continent,
        departure_date,
        arrival_airport,
        pilot_name,
        flight_status,
        ticket_type,
        passenger_status,
        METADATA$ACTION,
        METADATA$ISUPDATE,
        METADATA$ROW_ID,
        _ingested_at
    )
    SELECT
        passenger_id,
        first_name,
        last_name,
        gender,
        age,
        nationality,
        airport_name,
        airport_country_code,
        country_name,
        airport_continent,
        continent,
        departure_date,
        arrival_airport,
        pilot_name,
        flight_status,
        ticket_type,
        passenger_status,
        METADATA$ACTION,
        METADATA$ISUPDATE,
        METADATA$ROW_ID,
        _ingested_at
    FROM AIRLINE_DB.BRONZE.airline_raw_stream;

    

        MERGE INTO AIRLINE_DB.SILVER.airline_silver AS target
        USING (
            SELECT
                passenger_id,
                first_name,
                last_name,
                gender,
                TRY_CAST(age AS INT) AS age,
                nationality,
                airport_name,
                airport_country_code,
                country_name,
                airport_continent,
                continent,
                TRY_CAST(departure_date AS DATE) AS departure_date,
                arrival_airport,
                pilot_name,
                flight_status,
                ticket_type,
                passenger_status
            FROM AIRLINE_DB.SILVER.AIRLINE_STREAM_BUFFER
            WHERE METADATA$ACTION = 'INSERT'
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY passenger_id
                ORDER BY _ingested_at DESC
            ) = 1 
        ) AS source
        ON target.passenger_id = source.passenger_id

        WHEN MATCHED THEN
            UPDATE SET
                first_name           = source.first_name,
                last_name            = source.last_name,
                gender               = source.gender,
                age                  = source.age,
                nationality          = source.nationality,
                airport_name         = source.airport_name,
                airport_country_code = source.airport_country_code,
                country_name         = source.country_name,
                airport_continent    = source.airport_continent,
                continent            = source.continent,
                departure_date       = source.departure_date,
                arrival_airport      = source.arrival_airport,
                pilot_name           = source.pilot_name,
                flight_status        = source.flight_status,
                ticket_type          = source.ticket_type,
                passenger_status     = source.passenger_status,
                _last_updated        = CURRENT_TIMESTAMP()

        WHEN NOT MATCHED THEN
            INSERT (
                passenger_id,
                first_name,
                last_name,
                gender,
                age,
                nationality,
                airport_name,
                airport_country_code,
                country_name,
                airport_continent,
                continent,
                departure_date,
                arrival_airport,
                pilot_name,
                flight_status,
                ticket_type,
                passenger_status
            )
            VALUES (
                source.passenger_id,
                source.first_name,
                source.last_name,
                source.gender,
                source.age,
                source.nationality,
                source.airport_name,
                source.airport_country_code,
                source.country_name,
                source.airport_continent,
                source.continent,
                source.departure_date,
                source.arrival_airport,
                source.pilot_name,
                source.flight_status,
                source.ticket_type,
                source.passenger_status
            );

        rows_affected := SQLROWCOUNT;
        
        DELETE FROM AIRLINE_DB.SILVER.airline_stream_buffer;
        INSERT INTO AIRLINE_DB.DWH_AUDIT.PIPELINE_AUDIT_LOG
            (proc_name, affected_rows, status, run_by)
        VALUES
            (:proc_name, :rows_affected, 'SUCCESS', :current_user);
        COMMIT;
        RETURN 'SUCCESS | Rows merged: ' || rows_affected;

EXCEPTION
    WHEN OTHER THEN
        ROLLBACK;
        INSERT INTO AIRLINE_DB.DWH_AUDIT.PIPELINE_AUDIT_LOG
            (proc_name, affected_rows, status, error_message, run_by)
        VALUES
            (:proc_name, 0, 'FAILED', :SQLERRM, :current_user);

        RETURN 'FAILED | Error: ' || :SQLERRM;
END;




CALL sp_load_airline_data_silver();


--  CREATE A STREAM ON TABLE airline_silver SO WE COULD TRACK CDC 
CREATE OR REPLACE STREAM silver_data_stream ON TABLE AIRLINE_SILVER;

SELECT SYSTEM$STREAM_HAS_DATA('AIRLINE_DB.SILVER.silver_data_stream')::BOOLEAN;


SELECT * FROM airline_silver;


-- TRUNCATE TABLE airline_silver


