## Snowflake Task

This project implements a Medallion Architecture, where data flows through three layers: Bronze, Silver, and Gold.

The **`./snowflake`** directory contains all SQL scripts:

- **./snowflake/stage_1.sql**  
  Handles the Bronze layer. Creates raw tables and configures Snowpipe, external stage, and streams.

- **./snowflake/stage_2.sql**  
  Processes data from the Bronze stream and loads it into the Silver layer.

- **./snowflake/stage_3.sql**  
  Implements the Gold layer. Builds a star schema and secure views, and reads from a stream that tracks CDC changes on the Silver tables.

- **./snowflake/rls.sql**  
  Defines row‑level security policies on the fact table.

- **./snowflake/privileges.sql**  
  Grants the required permissions for all objects.

- **./snowflake/time_travel.sql**  
  Demonstrates time‑travel queries.

- **./snowflake/logging.sql**  
  Creates schemas and tables used to log the execution of procedures in the Silver and Gold layers.

**Airflow** orchestrates the pipeline.  
The DAG is triggered whenever a new message arrives from SQS (subscribed to the same SNS topic used by Snowpipe).  
When a new file lands in the S3 bucket, an SNS notification is sent, triggering both:

- Snowpipe
- The Airflow DAG
