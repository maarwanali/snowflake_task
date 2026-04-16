
--  /////   2 DDL queries using time-travel feature of Snowflake  ///////////

SELECT * FROM AIRLINE_DB.GOLD.fact_flight_data BEFORE (OFFSET => -3600);

-- ////
CREATE TABLE AIRLINE_DB.GOLD.fact_table_clone CLONE AIRLINE_DB.GOLD.fact_flight_data;  -- ZERO COPY CLONING (only copy the offset so not acctuly real cloning)
DROP TABLE AIRLINE_DB.GOLD.fact_table_clone;
UNDROP TABLE AIRLINE_DB.GOLD.fact_table_clone;



-- /////////// 2 Examples of DML time trivel//////////

DELETE FROM AIRLINE_DB.GOLD.fact_table_clone
WHERE flight_status ='Cancelled';

SELECT count(*) FROM AIRLINE_DB.GOLD.fact_table_clone;

INSERT INTO AIRLINE_DB.GOLD.fact_table_clone 
SELECT * 
FROM AIRLINE_DB.GOLD.fact_table_clone AT (OFFSET => -300); -- 5 mins = 5*60


-- ////  RESTORE UNCORRECT Updates
SELECT * FROM AIRLINE_DB.GOLD.dim_passenger; 

UPDATE AIRLINE_DB.GOLD.dim_passenger t
SET nationality= 'Unknown';


UPDATE AIRLINE_DB.GOLD.dim_passenger curr
SET nationality = old.nationality
FROM AIRLINE_DB.GOLD.dim_passenger 
AT(OFFSET => -360) old
WHERE curr.passenger_id = old.passenger_id

