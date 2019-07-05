from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.base_sensor_operator import BaseSensorOperator

from datetime import datetime, timedelta

import fx_functions as fx


default_args = {
    'start_date': datetime(2019, 7, 1),
    'retries': 31,
    'retry_delay': timedelta(days=1),}

dag = DAG(
    dag_id='fx_dag',
    default_args=default_args,
    schedule_interval='@monthly')

class data_availability_sensor(BaseSensorOperator):
    '''
    Invokes check_data_availability(), which returns a boolean
    indicating if the desired data is available.
    '''
    def __init__(self, *args, **kwargs):
        super(data_availability_sensor, self).__init__(*args, **kwargs)
    def poke(self, context):
        print('Poking {}'.__str__())
        return fx.check_data_availability(**context)

wait_for_data_availability_sensor = data_availability_sensor(
    task_id='wait_for_data_availability',
    dag=dag)

make_month_df_operator = PythonOperator(
    task_id='make_month_df',
    python_callable=fx.make_month_df,
    provide_context=True,
    dag=dag)

make_min_max_parquet_operator = PythonOperator(
    task_id='make_min_max_parquet',
    python_callable=fx.make_min_max_parquet,
    provide_context=True,
    dag=dag)

make_max_profit_parquet_operator = PythonOperator(
    task_id='make_max_profit_parquet',
    python_callable=fx.make_max_profit_parquet,
    provide_context=True,
    dag=dag)

wait_for_data_availability_sensor >> make_month_df_operator
make_month_df_operator >> [make_min_max_parquet_operator, make_max_profit_parquet_operator]
