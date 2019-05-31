rm -r ~/kite_meta
rm -r ~/kite_data
rm ~/airflow/airflow.db
rm -r ~/airflow/dags/
cd ~/airflow
~/conda/bin/airflow initdb
mkdir dags
cd -
rm input/*
