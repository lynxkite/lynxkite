'''Test DAG to produce input for LK2 pipeline tests.

The output of these pipelines depends on the actual time, and
not on the Airflow execution date.
'''


import subprocess
import datetime
import re
from airflow.operators import PythonOperator
from airflow.models import DAG


def filename(base, date):
  folder = '/tmp/cleaner_test'
  return f'{folder}/{base}-{date.strftime("%Y%m%d-%H%M")}.csv'


def get_index(**kwargs):
  nums = []
  cmd = subprocess.Popen('wget -O - https://tamogatas.index.hu | grep data-number-to | grep div',
                         shell=True,
                         stdout=subprocess.PIPE)
  for line in cmd.stdout:
    nums.append(int(re.search(r'\d+', line.decode('ascii').strip()).group(0)))
  with open(filename('index', kwargs['execution_date']), 'w') as f:
    f.write('supporters,amount\n')
    f.write(f'{nums[0]},{nums[1]}')
  return nums


def get_google(**kwargs):
  nums = []
  cmd = subprocess.Popen('wget -O -  https://api.iextrading.com/1.0/stock/googl/price | cat',
                         shell=True,
                         stdout=subprocess.PIPE)
  for line in cmd.stdout:
    nums.append(float(re.search(r'\d+.\d+', line.decode('ascii').strip()).group(0)))
  with open(filename('google', kwargs['execution_date']), 'w') as f:
    f.write('price\n')
    f.write(f'{nums[0]}')
  return nums


args = {
    'owner': 'airflow',
    'start_date': datetime.datetime(2018, 11, 21, 11, 0),
}

dag = DAG(
    dag_id='web_parser',
    default_args=args,
    schedule_interval='0 * * * *')

index_support = PythonOperator(
    task_id='get_index_amount',
    python_callable=get_index,
    provide_context=True,
    dag=dag)

google_price = PythonOperator(
    task_id='get_google_price',
    python_callable=get_google,
    provide_context=True,
    dag=dag)
