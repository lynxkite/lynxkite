# Simple ETL pipeline

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from os import path
import subprocess
import random
import lynx.kite

test_folder = path.dirname(path.realpath(__file__)) + '/etl_test/'
subprocess.check_call(['mkdir', '-p', test_folder])

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.now() - timedelta(minutes=5),
    'local_src_folder': test_folder,
    'lk_dst_folder': 'test/etl',
}

scheduling = '*/1 * * * *'

dag = DAG('etl_tasks', default_args=default_args, schedule_interval=scheduling)

date_format = '%Y-%m-%dT%H%M'

etl_rules = {
    'dollars': 'round(100*cast(dollars as double))/100',
    'gender': 'upper(trim(gender))',
}


def src_name(dt):
  date_part = dt.strftime(date_format)
  return 'source_{}.csv'.format(date_part)


def local_src_path(dt):
  return default_args['local_src_folder'] + src_name(dt)


def local_producer(ds, execution_date, **kwargs):
  with open(local_src_path(execution_date), 'w') as f:
    f.write('person_id,phone_no,gender,dollars\n')
    start_id = random.randint(1, 5)
    for i in range(1, 5):
      person_id = start_id + i
      phone_no = '+36201234567' if random.randint(0, 2) == 1 else '201234567'
      gender = 'm ' if random.randint(0, 2) == 1 else 'F'
      dollars = 100 * random.uniform(100.0, 600.0)
      row = ','.join([str(person_id), phone_no, gender, str(dollars)])
      f.write(row + '\n')


def load_and_standardize(ds, execution_date, **kwargs):
  lk = lynx.kite.LynxKite()
  with open(local_src_path(execution_date)) as f:
    prefixed_path = lk.upload(f)
  input_table = lk.importCSV(filename=prefixed_path)
  tss_result = lynx.kite.TableSnapshotSequence(default_args['lk_dst_folder'], scheduling)
  transform_args = {'new_' + key: transform for key, transform in etl_rules.items()}
  result = input_table.transform(**transform_args)
  result_id = lk.get_state_id(result)
  tss_result.save_to_sequence(lk, result_id, execution_date)


def extend_with_lookup(ds, execution_date, **kwargs):
  lk = lynx.kite.LynxKite()
  tss = lynx.kite.TableSnapshotSequence(default_args['lk_dst_folder'], scheduling)
  raw = tss.read_interval(lk, execution_date, execution_date)
  lookup = lk.importSnapshot(path='reference_tables/names')
  extended = lk.sql(
      'select * from one left join two on one.person_id=two.id', raw, lookup)
  result_id = lk.get_state_id(extended)
  result_tss = lynx.kite.TableSnapshotSequence(
      default_args['lk_dst_folder'] + '/extended', scheduling)
  result_tss.save_to_sequence(lk, result_id, execution_date)


def load_lookup(ds, execution_date, **kwargs):
  lk = lynx.kite.LynxKite()
  response = lk.get_directory_entry('reference_tables/names')
  if not (response.exists and response.isSnapshot):
    # Uploading / creating lookup table
    prefixed_path = lk.upload(
        'id,name\n1,Adam\n2,Bob\n3,Eve\n4,Isolated Joe\n5,Jesse\n6,Walt\n7,Mike')
    state_id = lk.get_state_id(lk.importCSV(filename=prefixed_path))
    lk.save_snapshot('reference_tables/names', state_id)


run_local_producer = PythonOperator(
    task_id='create_local_csv',
    provide_context=True,
    python_callable=local_producer,
    dag=dag,
)

run_load = PythonOperator(
    task_id='load_and_standardize_input',
    provide_context=True,
    python_callable=load_and_standardize,
    dag=dag,
)

run_load_lookup = PythonOperator(
    task_id='load_lookup',
    provide_context=True,
    python_callable=load_lookup,
    dag=dag,
)

run_extend = PythonOperator(
    task_id='extend_raw_table',
    provide_context=True,
    python_callable=extend_with_lookup,
    dag=dag,
)

run_local_producer >> run_load >> run_extend << run_load_lookup
