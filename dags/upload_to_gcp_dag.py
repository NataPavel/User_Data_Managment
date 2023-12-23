from airflow import DAG
from datetime import datetime

from operators.get_costs_operator import GetCostsOperator

# setting up DAG parametes
with DAG(
    dag_id="user_management",
    tags=['test_task'],
    start_date=datetime.now(),
    max_active_runs=1,
    catchup=False,
    # starting dag daily
    schedule_interval='0 0 * * *',
) as dag:
     # using custom operator to get data from of costs API
    get_costs_task = GetCostsOperator(
        task_id="get_costs",
    )

get_costs_task