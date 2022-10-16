from airflow import DAG
from airflow.operators.python import PythonOperator

from airflow_utils.commands import run_mbank, run_baselinker
from airflow_utils.defaults import get_default_airflow_config


with DAG(
    'mbank',
    **get_default_airflow_config(),
) as mbank_dag:

    mbank = PythonOperator(
        task_id='refresh_mbank_parser',
        python_callable=run_mbank,
    )

with DAG(
    'baselinker',
    **get_default_airflow_config(),
) as baselinker_dag:

    baselinker = PythonOperator(
        task_id='refresh_baselinker',
        python_callable=run_baselinker,
    )
