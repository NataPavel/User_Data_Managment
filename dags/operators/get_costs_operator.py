import json
import os

from airflow import AirflowException
from airflow.models import BaseOperator
from airflow.providers.http.hooks.http import HttpHook
from airflow.models import Variable


class GetCostsOperator(BaseOperator):
    def __init__(self, task_id: str, **kwargs):
        super().__init__(task_id=task_id, **kwargs)

    def execute(self, context):
        execution_date = context['execution_date']
        hook = HttpHook(
            method='GET',
            http_conn_id='api_data'
        )

        date_str = execution_date.strftime("%Y-%m-%d")
        page = 1
        auth_token = Variable.get('HOLY_WATER_API_TOKEN')
        while True:
            try:
                response = hook.run(
                    endpoint=f"/costs?date={date_str}&page={page}",
                    headers={'Authorization': auth_token}
                )
                response.raise_for_status()

                raw_dir = f'/opt/airflow/data/{date_str}'

                if not os.path.exists(raw_dir):
                    os.makedirs(raw_dir)

                if response.status_code == 200:
                    data = response.json()
                    # Store the data in raw_dir as JSON
                    with open(f"{raw_dir}/costs_{date_str}_page{page}.csv", 'w') as file:
                        json.dump(data, file)
                    page += 1
                elif response.status_code == 201:
                    break
                else:
                    raise AirflowException(f"Failed to retrieve data from API. Status code: {response.status_code}")
            except AirflowException as e:
                if "404" in str(e):
                    print(f"Data not found for date={date_str} and page={page}. Skipping...")
                    break
                else:
                    raise