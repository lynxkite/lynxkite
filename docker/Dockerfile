# Single-machine LynxKite image.
FROM mambaorg/micromamba:0.25.1

# Install dependencies early so they are in a low layer.
COPY lynxkite-env.yml /
RUN micromamba install -y -n base -f /lynxkite-env.yml && \
    micromamba clean --all --yes

USER root
# Set --build-arg KITE_ENABLE_CUDA=yes to add CUDA dependencies too.
ARG KITE_ENABLE_CUDA=no
ENV KITE_ENABLE_CUDA=$KITE_ENABLE_CUDA
# The user has to configure these.
ENV KITE_MASTER_MEMORY_MB 1024
ENV SPHYNX_CACHED_ENTITIES_MAX_MEM_MB 1024
VOLUME /data
VOLUME /metadata

# Add a dependency missed by Conda. (https://github.com/rapidsai/ucx-py/issues/790)
RUN if [ $KITE_ENABLE_CUDA = yes ]; then \
  apt-get update && apt-get install -y libnuma-dev && rm -rf /var/lib/apt/lists/*; fi

# Install LynxKite.
COPY lynxkite.jar /
COPY run.sh /

# We add Conda to LD_LIBRARY_PATH to make NetworKit available.
# But then system binaries start using the wrong libtinfo.so.6.
RUN rm /opt/conda/lib/libtinfo.so.6
ENV LD_LIBRARY_PATH /opt/conda/lib
ENV ADD_TO_PYTHON_JAIL /opt/conda/lib

CMD ["/run.sh"]

EXPOSE 2200
