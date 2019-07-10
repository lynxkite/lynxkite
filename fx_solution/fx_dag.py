from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.base_sensor_operator import BaseSensorOperator

from datetime import datetime, timedelta

import fx_functions as fx


default_args = {
    'start_date': datetime(2019, 7, 1),
    'retries': 1,
    'retry_delay': timedelta(days=1), }

dag = DAG(
    dag_id='fx_dag',
    default_args=default_args,
    schedule_interval='@monthly')


class DataAvailabilitySensor(BaseSensorOperator):
  '''
  Invokes check_data_availability(), which returns a boolean
  indicating if the desired data is available.
  '''

  def __init__(self, *args, **kwargs):
    super(DataAvailabilitySensor, self).__init__(*args, **kwargs)

  def poke(self, context):
    return fx.check_data_availability(date=context['ds'])


wait_for_data_availability_sensor = DataAvailabilitySensor(
    task_id='wait_for_data_availability',
    dag=dag)

make_month_df_operator = PythonOperator(
    task_id='make_month_df',
    python_callable=fx.make_month_df,
    op_kwargs={'date': '{{ ds }}'},
    dag=dag)

make_min_max_parquet_operator = PythonOperator(
    task_id='make_min_max_parquet',
    python_callable=fx.make_min_max_parquet,
    op_kwargs={'date': '{{ ds }}'},
    dag=dag)

make_max_profit_parquet_operator = PythonOperator(
    task_id='make_max_profit_parquet',
    python_callable=fx.make_max_profit_parquet,
    op_kwargs={'date': '{{ ds }}'},
    dag=dag)

wait_for_data_availability_sensor >> make_month_df_operator
make_month_df_operator >> [make_min_max_parquet_operator, make_max_profit_parquet_operator]
