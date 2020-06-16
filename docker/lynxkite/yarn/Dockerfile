# LynxKite on YARN image. Must run with "host" networking.
FROM java:8-jre

# Add system dependencies.
RUN apt-get update
RUN apt-get install -y python
RUN rm -rf /var/lib/apt/lists/*

# Install Spark early so it's in a low layer.
COPY stage/conf /lynxkite/conf
COPY stage/tools /lynxkite/tools
RUN /lynxkite/tools/install_spark.sh /spark

# The user has to configure these.
ENV KITE_INSTANCE=
ENV KITE_DATA_DIR=
ENV KITE_EPHEMERAL_DATA_DIR=
ENV EXECUTOR_MEMORY 10g
ENV NUM_EXECUTORS 2
ENV NUM_CORES_PER_EXECUTOR 4
ENV KITE_MASTER_MEMORY_MB 1024
VOLUME /hadoop_conf
VOLUME /metadata

# Install LynxKite.
COPY stage /lynxkite
RUN touch /prefix_definitions.txt
COPY run.sh /run.sh

ENTRYPOINT ["/run.sh"]
CMD ["interactive"]

EXPOSE 2200
