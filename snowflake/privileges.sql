CREATE ROLE airline_role;


-- Database visibility
GRANT USAGE ON DATABASE AIRLINE_DB TO ROLE airline_role;

-- Schemas (RAW / SILVER / GOLD)
GRANT USAGE ON SCHEMA AIRLINE_DB.bronze TO ROLE airline_role;
GRANT USAGE ON SCHEMA AIRLINE_DB.silver TO ROLE airline_role;
GRANT USAGE ON SCHEMA AIRLINE_DB.gold TO ROLE airline_role;

-- ///////////////     STREAMS ////////////////////////
GRANT SELECT ON ALL STREAMS IN SCHEMA airline_db.bronze TO ROLE airline_role;
GRANT SELECT ON FUTURE STREAMS IN SCHEMA airline_db.bronze TO ROLE airline_role;


GRANT SELECT ON ALL STREAMS IN SCHEMA airline_db.silver TO ROLE airline_role;
GRANT SELECT ON FUTURE STREAMS IN SCHEMA airline_db.silver TO ROLE airline_role;

-- /////////////      PROCEDURES      ///////////////////
GRANT USAGE ON ALL PROCEDURES IN SCHEMA airline_db.bronze TO ROLE airline_role;
GRANT USAGE ON ALL PROCEDURES IN SCHEMA airline_db.silver TO ROLE airline_role;
GRANT USAGE ON ALL PROCEDURES IN SCHEMA airline_db.gold TO ROLE airline_role;

GRANT USAGE ON FUTURE PROCEDURES IN SCHEMA airline_db.bronze TO ROLE airline_role;
GRANT USAGE ON FUTURE PROCEDURES IN SCHEMA airline_db.silver TO ROLE airline_role;
GRANT USAGE ON FUTURE PROCEDURES IN SCHEMA airline_db.gold TO ROLE airline_role;


GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA airline_db.silver TO ROLE airline_role;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA airline_db.gold TO ROLE airline_role;



-- /////////////      Tables      ///////////////////
GRANT SELECT ON ALL TABLES IN SCHEMA airline_db.bronze TO ROLE airline_role;
GRANT SELECT ON ALL TABLES IN SCHEMA airline_db.silver TO ROLE airline_role;
GRANT SELECT ON ALL TABLES IN SCHEMA airline_db.gold TO ROLE airline_role;


GRANT SELECT ON FUTURE TABLES IN SCHEMA airline_db.bronze TO ROLE airline_role;
GRANT SELECT ON FUTURE TABLES IN SCHEMA airline_db.silver TO ROLE airline_role;
GRANT SELECT ON FUTURE TABLES IN SCHEMA airline_db.gold TO ROLE airline_role;


--  ////////////////  VIEWS  /////////////////////
GRANT SELECT ON ALL VIEWS IN SCHEMA airline_db.gold TO ROLE airline_role;
GRANT SELECT ON FUTURE VIEWS IN SCHEMA airline_db.gold TO ROLE airline_role;


--  //////////////   WAREHOUSE ACCESS    ///////////////////

GRANT USAGE ON WAREHOUSE MY_WH TO ROLE airline_role;



--  ///////// CREATE the USER and Grant him the Role
CREATE USER airline_user
  PASSWORD = '******************'
  DEFAULT_ROLE = airline_role
  DEFAULT_WAREHOUSE = MY_WH
  MUST_CHANGE_PASSWORD = FALSE;

GRANT ROLE airline_role TO USER airline_user;



