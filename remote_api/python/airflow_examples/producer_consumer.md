## How to install Airflow

``sudo pip3 install apache-airflow[all]``

## How to try out the example

1. Create and airflow directory and `export AIRFLOW_HOME=...`
2. `airflow initdb`
3. `mkdir $AIRFLOW_HOME/dags`
4. Copy the example file to `$AIRFLOW_HOME/dags`
5. Start LK
6. Update `local_src_folder` , `lk_src_folder` and `lk_dst_folder`
in the Python file as you like.
7. `export PYTHONPATH=...` to include the location of the Python API
8. Start airflow (`airflow scheduler` and `airflow webserver`
  in two different terminals)
9. Turn on the `producer_consumer_tasks` DAG on the Airflow UI
10. Check the result snapshots in LynxKite

**Warning**:  the Airflow scheduler will backfill from `start_date`, so
it can create a lot of snaphots if `start_date` is way before the date
of the test run.
