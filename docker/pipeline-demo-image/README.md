Using this pipeline-demo docker image
===============================

This is a base image for pipeline demos. See the instructions of one of the demos
(e.g. `bus-demo-sub`) for how to build and start a useful image.

When you run this image, it will provide four services to the outside world:

1. LynxKite (available at port 2200)

2. Airflow (available at port 8080)

3. Jupyter (available at port 9596)

4. Supervisor (available at port 9001)

5. Postgres (available at port 5432)


No authentication is required for the first four.
For Postgres, the database name is "kite", the user is "kite" and the password is also "kite"

So, inside the image there is a running postgres database server. This is for LynxKite, so
that we can export and import tables to and from the database. I tested this from
a Jupyter notebook and it works:

    import lynx.kite
    lk = lynx.kite.LynxKite()
    create_example_graph_1 = lk.createExampleGraph()
    sql1_1 = lk.sql1(create_example_graph_1, sql='select name,age from vertices')
    export_to_jdbc_1 = lk.exportToJDBC(sql1_1, jdbc_url='jdbc:postgresql://localhost:5432/kite?user=kite&password=kite', jdbc_table='output')


Use the `./run.sh` script in this directory to start the container. The `dags` directory here
is mounted into `/tmp/mount/dags`; that's where the airflow scheduler will find your dags.

## Notebooks
The directory `notebooks` has some useful scripts. After you start the container,
run notebook `lk_database.ipynb`: this will use LK to create a table `output` in
the database. This table then will be read back to check that everything is OK.

Notebook `database.ipynb` can be run after you created the table. It is a simple
Python code that can easily be changed if you need to access the database from Python.

## Postgres client
A postgres client is installed in the image. Example usage:

    PGPASSWORD=kite psql -U kite -h localhost -p 5432 kite
    select * from output;

## Restarting any services (e.g., Airflow)
Go to the Supervisor Web UI (localhost:9001) and click on restart.




