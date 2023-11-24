from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from includes.vs_modules.merged_data import merge

#https://www.youtube.com/watch?v=IsWfoXY_Duk

args = {
    'owner': 'Group 3',
    'start_date': days_ago(1) # make start date in the past
}

dag = DAG(
    dag_id='crm-elastic-dag',
    default_args=args,
    schedule_interval='2 * * * *' # make this workflow happen every 2 minute
)

with dag:
    merge = PythonOperator(
        task_id='merge',
        python_callable=merge,
        # provide_context=True
    )