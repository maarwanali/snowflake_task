-- /////////////////////////////////////////////////////////////////////////////////////////////////
-- /////////////Create Secure View for any fact table and attach Row Level Security policy /////////
-- /////////////////////////////////////////////////////////////////////////////////////////////////

SELECT * FROM AIRLINE_DB.GOLD.FACT_FLIGHT_DATA;

CREATE OR REPLACE ROW ACCESS POLICY AIRLINE_DB.GOLD.role_access_policy AS (passenger_id VARCHAR)
RETURNS BOOLEAN ->
    CASE 
        WHEN CURRENT_ROLE() IN ('SYSADMIN', 'ACCOUNTADMIN','AIRLINE_ROLE','DATA_ANALYST') THEN TRUE
        ELSE FALSE
    END;
    

ALTER TABLE AIRLINE_DB.GOLD.FACT_FLIGHT_DATA
ADD ROW ACCESS POLICY AIRLINE_DB.GOLD.role_access_policy on (passenger_id);





