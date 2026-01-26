USE DATABASE AIRLINE_DB;

CREATE OR REPLACE SCHEMA GOLD;

--  CREATE a STAR SCHMEA module (each row represent passinger flight airport )fact table (Passengers, Flight, Airport denture, Airport arrivle, )+ Dims Tables

-- FACT TABLE 
-- passenger sug kay
-- airport sug key
-- airport arrivel sug key
-- flight info sugg key

CREATE OR REPLACE TABLE AIRLINE_DB.GOLD.fact_flight_data(
    passenger_sk INT,
    FOREIGN KEY(passenger_sk) REFERENCES dim_passenger(passenger_sk),
    departure_airport_sk INT,
    FOREIGN KEY(departure_airport_sk) REFERENCES dim_airport(airport_sk),
    arrival_airport_sk INT,
    FOREIGN KEY(arrival_airport_sk) REFERENCES dim_arrival_airport(arrival_airport_sk),
    pilot_sk INT,
    FOREIGN KEY(pilot_sk) REFERENCES dim_pilot(pilot_sk),
    ticket_sk INT,
    FOREIGN KEY(ticket_sk) REFERENCES dim_ticket_type(ticket_sk),
    pass_status_sk INT,
    FOREIGN KEY(pass_status_sk) REFERENCES dim_passenger_status(pass_status_sk),

    passenger_id STRING,
    flight_status STRING,

    original_departure_date DATE,
    current_departure_date DATE,
    update_count INT DEFAULT 0,
    last_record_update TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Dims Tables  : for each one of them add sugg key 

-- Passenger Info----------------------
-- passenger_id,
-- first_name,
-- last_name,
-- gender,
-- age,
-- nationality,

CREATE OR REPLACE TABLE dim_passenger (
    passenger_sk INT PRIMARY KEY IDENTITY(1,1),
    passenger_id STRING,
    first_name STRING,
    last_name STRING,
    gender STRING,
    age INT,
    nationality STRING,
    effective_date DATE,
    expiry_date DATE,
    current_flag BOOLEAN
);

SELECT * FROM dim_ticket_type;


-- Airport Info--------------------
-- airport_name,
-- airport_country_code,
-- country_name,
-- airport_continent,
-- continent,
                                        -- /////       Try to fit role playing dimention type       /////
-- Airport arrivle Info ---------------------
-- arrival_airport,
CREATE OR REPLACE TABLE dim_airport (
    airport_sk INT PRIMARY KEY IDENTITY(1,1),
    airport_name STRING,
    airport_country_code STRING,
    country_name STRING,
    airport_continent STRING,
    continent STRING
);

CREATE OR REPLACE TABLE dim_arrival_airport (
    arrival_airport_sk INT PRIMARY KEY IDENTITY(1,1),
    airport_code STRING
);



-- Flight info-------------------------------------
-- departure_date,
-- pilot_name,
-- flight_status,
-- ticket_type,
-- passenger_status

CREATE OR REPLACE TABLE dim_pilot (
    pilot_sk INT PRIMARY KEY IDENTITY(1,1),
    pilot_name STRING
);

CREATE OR REPLACE TABLE dim_ticket_type(
    ticket_sk INT PRIMARY KEY IDENTITY(1,1),
    ticket_type STRING
);

CREATE OR REPLACE TABLE dim_passenger_status(
    pass_status_sk INT PRIMARY KEY IDENTITY(1,1),
    passenger_status STRING
);

SELECT * FROM dim_airport;


 -- //////////////////////////////////////////////////////////////////////////
 -- ////  Handle the DATA STREAM FROM Silver Layer to Gold Layer
 -- //////////////////////////////////////////////////////////////////////////

SELECT * FROM AIRLINE_DB.SILVER.SILVER_DATA_STREAM;

 CREATE OR REPLACE PROCEDURE AIRLINE_DB.GOLD.sp_stream_data_gold_layer()
 RETURNS STRING
 LANGUAGE SQL
 AS
DECLARE
    proc_name STRING DEFAULT 'sp_stream_data_golde_layer';
    rows_affected INTEGER DEFAULT 0;
    current_user STRING DEFAULT CURRENT_USER();
 BEGIN
     --  check if stream has data
   IF (NOT SYSTEM$STREAM_HAS_DATA('AIRLINE_DB.SILVER.SILVER_DATA_STREAM')) THEN
        RETURN 'NO DATA | Stream is empty. Procedure skipped.';
    END IF;

    BEGIN TRANSACTION;

    CREATE OR REPLACE TEMP TABLE tmp_stream_data AS
    SELECT *
    FROM AIRLINE_DB.SILVER.SILVER_DATA_STREAM
    WHERE METADATA$ACTION = 'INSERT';
    
    -- //////////////// START MERGE /////////////////////

    --  /////////////////////////////////////////////////////////////////////////////////////////////////////////////// handle airport dim
    MERGE INTO AIRLINE_DB.GOLD.dim_airport AS target
    USING (
        SELECT DISTINCT
            airport_name,
            airport_country_code,
            country_name,
            airport_continent,
            continent
        FROM tmp_stream_data
    ) AS source
    ON target.airport_name = source.airport_name
    WHEN MATCHED THEN
        UPDATE SET
            target.airport_country_code = source.airport_country_code,
            target.country_name        = source.country_name,
            target.airport_continent   = source.airport_continent,
            target.continent           = source.continent
    
    WHEN NOT MATCHED THEN
        INSERT (
            airport_name,
            airport_country_code,
            country_name,
            airport_continent,
            continent
        )
        VALUES (
            source.airport_name,
            source.airport_country_code,
            source.country_name,
            source.airport_continent,
            source.continent
        );
    
    


    MERGE INTO AIRLINE_DB.GOLD.dim_arrival_airport AS target
    USING(
        SELECT 
            DISTINCT arrival_airport 
        FROM 
           tmp_stream_data
    ) AS source
    ON target.airport_code = source.arrival_airport
    WHEN NOT MATCHED THEN
            INSERT(airport_code) VALUES(source.arrival_airport);


    -- /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////handle passenger dim WITH SCD type 2 saving historical data 

    UPDATE AIRLINE_DB.GOLD.dim_passenger target
    SET
        target.expiry_date = CURRENT_DATE(),
        target.current_flag = FALSE
    FROM 
       tmp_stream_data source
    WHERE
        target.passenger_id = source.passenger_id AND
        target.current_flag=True AND
        HASH(target.first_name,target.last_name, target.gender, target.age, target.nationality)
        <> HASH(source.first_name,source.last_name, source.gender, source.age, source.nationality);
    
      INSERT INTO AIRLINE_DB.GOLD.dim_passenger (
        passenger_id, first_name, last_name, gender, age, nationality,
        effective_date, expiry_date, current_flag
    )
    SELECT 
        s.passenger_id, s.first_name, s.last_name, s.gender, s.age, s.nationality,
        CURRENT_DATE(), NULL, TRUE
    FROM (SELECT DISTINCT * FROM tmp_stream_data) s
    LEFT JOIN AIRLINE_DB.GOLD.dim_passenger p
        ON p.passenger_id = s.passenger_id AND p.current_flag = TRUE
    WHERE p.passenger_id IS NULL
       OR HASH(
            p.first_name, p.last_name, p.gender, p.age, p.nationality
          ) <> HASH(
            s.first_name, s.last_name, s.gender, s.age, s.nationality
          );

    
    -- //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////handle pilot dim
   MERGE INTO AIRLINE_DB.GOLD.dim_pilot t
    USING (SELECT DISTINCT pilot_name FROM tmp_stream_data) s
    ON t.pilot_name = s.pilot_name
    WHEN NOT MATCHED THEN
        INSERT (pilot_name) VALUES (s.pilot_name);

    -- //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////handle ticket dim
    MERGE INTO AIRLINE_DB.GOLD.dim_ticket_type t
    USING (SELECT DISTINCT ticket_type FROM tmp_stream_data) s
    ON t.ticket_type = s.ticket_type
    WHEN NOT MATCHED THEN
        INSERT (ticket_type) VALUES (s.ticket_type);

    -- //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////handle passenger status dim

 MERGE INTO AIRLINE_DB.GOLD.dim_passenger_status t
    USING (SELECT DISTINCT passenger_status FROM tmp_stream_data) s
    ON t.passenger_status = s.passenger_status
    WHEN NOT MATCHED THEN
        INSERT (passenger_status) VALUES (s.passenger_status);


    -- ///////////////////////////////////handle fact table with left join so it will handle missing data from   -the dim tables 

    
   MERGE INTO AIRLINE_DB.GOLD.fact_flight_data t
    USING (
        SELECT DISTINCT
            p.passenger_sk,
            da.airport_sk           AS departure_airport_sk,
            aa.arrival_airport_sk   AS arrival_airport_sk,
            pi.pilot_sk,
            tt.ticket_sk,
            ps.pass_status_sk,
            s.departure_date,
            s.flight_status,
            s.passenger_id
        FROM tmp_stream_data s
        LEFT JOIN AIRLINE_DB.GOLD.dim_passenger p
            ON s.passenger_id = p.passenger_id AND p.current_flag = TRUE
        LEFT JOIN AIRLINE_DB.GOLD.dim_airport da
            ON s.airport_name = da.airport_name
        LEFT JOIN AIRLINE_DB.GOLD.dim_arrival_airport aa
            ON s.arrival_airport = aa.airport_code
        LEFT JOIN AIRLINE_DB.GOLD.dim_pilot pi
            ON s.pilot_name = pi.pilot_name
        LEFT JOIN AIRLINE_DB.GOLD.dim_ticket_type tt
            ON s.ticket_type = tt.ticket_type
        LEFT JOIN AIRLINE_DB.GOLD.dim_passenger_status ps
            ON s.passenger_status = ps.passenger_status
    ) s
        ON  t.passenger_id = s.passenger_id
        AND t.departure_airport_sk = s.departure_airport_sk
        AND t.original_departure_date = s.departure_date

    WHEN MATCHED AND t.current_departure_date <> s.departure_date THEN
        UPDATE SET
            current_departure_date = s.departure_date,
            flight_status          = s.flight_status,
            update_count           = t.update_count + 1,
            last_record_update     = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN
        INSERT (
            passenger_sk, departure_airport_sk, arrival_airport_sk,
            pilot_sk, ticket_sk, pass_status_sk,
            original_departure_date, current_departure_date,
            flight_status, passenger_id
        )
        VALUES (
            s.passenger_sk, s.departure_airport_sk, s.arrival_airport_sk,
            s.pilot_sk, s.ticket_sk, s.pass_status_sk,
            s.departure_date, s.departure_date,
            s.flight_status, s.passenger_id
        );
    

    -- /////////////// END///////////////////////////////

    rows_affected := SQLROWCOUNT;
    
    INSERT INTO AIRLINE_DB.DWH_AUDIT.PIPELINE_AUDIT_LOG 
        (proc_name, affected_rows, status, run_by)
    VALUES
        (:proc_name, :rows_affected,'SUCCESS',:current_user);
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
-- /////////////////////////////////////////////////////////
-- //////////////////////// END PROCEDURE/////////////////
-- /////////////////////////////////////////////////////////

 CALL sp_stream_data_gold_layer();

 SELECT * FROM fact_flight_data;



-- //////////////////////////////////////////////////////////////////////////////////
-- //////////////////// Views ///////////////////////////////////////////////////////
-- //////////////////////////////////////////////////////////////////////////////////


CREATE OR REPLACE SECURE VIEW get_delayed_flights AS
SELECT
    daa.airport_code as departure_airport,
    da.airport_name as arrival_airport_name,
    da.country_name as country_name,
    ff.ticket_sk,
    ff.original_departure_date,
    ff.current_departure_date
    
FROM
    fact_flight_data ff
LEFT JOIN
    dim_airport da ON ff.arrival_airport_sk = da.airport_sk
LEFT JOIN 
    dim_arrival_airport daa ON ff.departure_airport_sk = daa.arrival_airport_sk 
WHERE
    ff.flight_status = 'Delayed';


SELECT * FROM get_delayed_flights;






-- TRUNCATE TABLE dim_passenger;
-- TRUNCATE TABLE dim_airport;
-- TRUNCATE TABLE dim_arrival_airport;
-- TRUNCATE TABLE dim_passenger_status;
-- TRUNCATE TABLE dim_pilot;
-- TRUNCATE TABLE dim_ticket_type;
-- TRUNCATE TABLE fact_flight_data;
