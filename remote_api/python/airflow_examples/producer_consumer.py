# Simple PythonOperator to create input files

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from os import path
import subprocess
import random
import lynx.kite

test_folder = path.dirname(path.realpath(__file__)) + '/prod_cons_test/'
subprocess.check_call(['mkdir', '-p', test_folder])

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.now() - timedelta(minutes=10),
    'local_src_folder': test_folder,
    'lk_src_folder': 'test/src',
    'lk_dst_folder': 'test/dst',
}

scheduling = '*/2 * * * *'

dag = DAG('producer_consumer_tasks', default_args=default_args, schedule_interval=scheduling)

date_format = '%Y-%m-%dT%H%M'


def src_name(dt):
  date_part = dt.strftime(date_format)
  return 'source_{}.csv'.format(date_part)


def local_src_path(dt):
  return default_args['local_src_folder'] + src_name(dt)


def lk_src_path(dt):
  return default_args['lk_src_folder'] + dt.strftime(date_format)


def local_producer(ds, **kwargs):
  with open(local_src_path(kwargs['execution_date']), 'a') as f:
    f.write('date_id,val\n')
    f.write(str(kwargs['execution_date']) + ',' + str(random.randint(1, 1234567)) + '\n')


def lk_producer(ds, **kwargs):
  data = 'date_id,val\n{},{}\n'.format(
      str(kwargs['execution_date']), str(random.randint(1, 1234567)))
  lk = lynx.kite.LynxKite()
  path = lk.upload(data)
  state = lk.importCSV(filename=path)
  state_id = lk.get_state_id(state)
  tss = lynx.kite.TableSnapshotSequence(default_args['lk_src_folder'], scheduling)
  tss.save_to_sequence(lk, state_id, kwargs['execution_date'])


def lk_consumer(ds, **kwargs):
  lk = lynx.kite.LynxKite()
  dt = kwargs['execution_date']
  with open(local_src_path(dt)) as f:
    prefixed_path = lk.upload(f)
  table1 = lk.importCSV(filename=prefixed_path)
  tss = lynx.kite.TableSnapshotSequence(default_args['lk_src_folder'], scheduling)
  tss_result = lynx.kite.TableSnapshotSequence(default_args['lk_dst_folder'], scheduling)
  table2 = tss.read_interval(lk, dt, dt)
  result = lk.sql('select * from one union all select * from two', table1, table2)
  result_id = lk.get_state_id(result)
  tss_result.save_to_sequence(lk, result_id, dt)


run_local_producer = PythonOperator(
    task_id='create_local_csv',
    provide_context=True,
    python_callable=local_producer,
    dag=dag,
)

run_lk_producer = PythonOperator(
    task_id='create_lk_snapshot',
    provide_context=True,
    python_callable=lk_producer,
    dag=dag,
)

run_lk_consumer = PythonOperator(
    task_id='load_and_use_inputs',
    provide_context=True,
    python_callable=lk_consumer,
    dag=dag,
)

run_local_producer >> run_lk_producer >> run_lk_consumer
