## How to install Airflow

``sudo pip3 install apache-airflow[all]``

## How to try out the example

1. Create and airflow directory and `export AIRFLOW_HOME=...`
1. `airflow initdb`
1. `mkdir $AIRFLOW_HOME/dags`
1. Copy the example file to `$AIRFLOW_HOME/dags`
1. Start LK
1. Update `local_src_folder` , `lk_src_folder` and `lk_dst_folder`
in the Python file as you like.
1. `export PYTHONPATH=...` to include the location of the Python API
1. Start airflow (`airflow scheduler` and `airflow webserver`
  in two different terminals)
1. Turn on the `producer_consumer_tasks` DAG on the Airflow UI
1. Check the result snapshots in LynxKite

**Warning**:  the Airflow scheduler will backfill from `start_date`, so
it can create a lot of snaphots if `start_date` is way before the date
of the test run.
