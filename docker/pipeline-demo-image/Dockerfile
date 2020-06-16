# Image for Python 3.6, Java 8, Airflow, Jupyter and Postgres
FROM openjdk:8-jre

WORKDIR /
ENV USER root
ENV SLUGIFY_USES_TEXT_UNIDECODE yes
ENV AIRFLOW_HOME /tmp/airflow

# Install Spark early, so the layer doesn't need to pushed so often.
ADD build/stage/conf/SPARK_VERSION /tmp/build/stage/conf/SPARK_VERSION
ADD build/stage/tools/install_spark.sh /tmp/build/stage/tools/install_spark.sh
RUN HOME=/tmp /tmp/build/stage/tools/install_spark.sh

RUN \
  apt-get update -y && \
  apt-get install -y apt-utils  wget gcc libyaml-dev netcat \
          postgresql default-libmysqlclient-dev libpq-dev postgresql-client python2.7

RUN \
  curl https://bootstrap.pypa.io/get-pip.py | python2.7 && \
  pip2.7 install supervisor


# LK requires Python 3.6
# We are using Anaconda Python 3.6 here.
RUN \
  wget -nv 'https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh' -O conda.sh && \
  bash conda.sh -b -p /opt/conda && \
  rm conda.sh && \
  /opt/conda/bin/python3 -c "import sys; assert(sys.version_info.minor >= 6)" && \
  /opt/conda/bin/python3 /opt/conda/bin/conda install python=3.6 && \
  /opt/conda/bin/pip install jupyterlab


#Apparently this is necessary.
RUN ln -s /opt/conda/bin/airflow /bin/airflow

ADD build /tmp/build
RUN /opt/conda/bin/pip install /tmp/build/python/remote_api
RUN /opt/conda/bin/pip install /tmp/build/python/automation
RUN /opt/conda/bin/pip install matplotlib psycopg2

# Postgres
USER postgres
RUN /etc/init.d/postgresql start && \
    psql --command "CREATE USER kite WITH SUPERUSER PASSWORD 'kite';" && \
    createdb -O kite kite && \
    createdb -O kite airflow

USER root
ENV JAVA_HOME /usr/local/openjdk-8/

RUN airflow initdb && \
    sed "s/load_examples = True/load_examples = False/" -i /tmp/airflow/airflow.cfg  && \
    sed "s/dag_concurrency =.*/dag_concurrency = 1/" -i /tmp/airflow/airflow.cfg  && \
    sed "s/max_active_runs_per_dag =.*/max_active_runs_per_dag = 1/" -i /tmp/airflow/airflow.cfg  && \
    sed "s:dags_folder = /tmp/airflow/dags:dags_folder = /tmp/mount/dags:" -i /tmp/airflow/airflow.cfg && \
    sed "s/executor = SequentialExecutor/executor = LocalExecutor/" -i /tmp/airflow/airflow.cfg && \
    sed "s-sql_alchemy_conn = .*-sql_alchemy_conn = postgresql+psycopg2://kite:kite@localhost/airflow-" -i /tmp/airflow/airflow.cfg && \
    sed "s/parallelism =.*/parallelism = 4/" -i /tmp/airflow/airflow.cfg

# Needed for LocalExecutor
RUN /etc/init.d/postgresql start && sleep 100 && airflow resetdb -y


ADD kiterc /tmp/
RUN mkdir -p /tmp/mount/dags
ADD notebooks /tmp/mount/notebooks
ADD scripts /tmp/scripts
ADD start.sh /tmp/
ADD supervisord.conf /tmp/
CMD /tmp/start.sh
