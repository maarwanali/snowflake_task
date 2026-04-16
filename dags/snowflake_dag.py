from airflow.sdk import dag, task, task_group, AssetWatcher, Asset
from airflow.providers.common.messaging.triggers.msg_queue import MessageQueueTrigger
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.sdk.bases.sensor import PokeReturnValue
from airflow.sdk import Variable
from datetime import datetime
import logging

trigger = MessageQueueTrigger(scheme='sqs', 
                              queue=Variable.get('sqs_arn'), 
                              aws_conn_id='aws_conn',  
                              region_name="eu-north-1")

asset  = Asset(
    "sqs_queue_asset", watchers=[AssetWatcher(name='sqs_watcher', trigger=trigger)]
)

@dag(schedule=[asset],start_date=datetime(2025, 1, 1), catchup=False)
def snowflake_dag():
    @task
    def start():
        return 'Dag is started wating for stream'


    @task.sensor(poke_interval=10, timeout=3600,mode='reschedule' )
    def wait_silver_stream():
        hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
        sql = """ SELECT SYSTEM$STREAM_HAS_DATA('AIRLINE_DB.BRONZE.AIRLINE_RAW_STREAM')::BOOLEAN; """        
        result = hook.get_first(sql)

        if result[0] == True:
            condition_met = True
            operator_return_value= result
        else:
            condition_met = False
            operator_return_value= None

        return PokeReturnValue(is_done=condition_met, xcom_value=operator_return_value)


 
    

    load_data_stream_to_silver = SQLExecuteQueryOperator(
        task_id="call_stored_procedure_silver",
        conn_id="snowflake_conn",  
        sql="CALL AIRLINE_DB.SILVER.SP_LOAD_AIRLINE_DATA_SILVER();" 
    )

    load_data_stream_to_gold = SQLExecuteQueryOperator(
        task_id="call_stored_procedure_gold",
        conn_id="snowflake_conn",  
        sql="CALL AIRLINE_DB.gold.sp_stream_data_gold_layer();" 
    )

  

    @task
    def end():
        return 'Dag finshed successfully'
    


    start() >> wait_silver_stream() >> load_data_stream_to_silver >>load_data_stream_to_gold >>end()




snowflake_dag()