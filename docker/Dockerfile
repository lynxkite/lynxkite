# Single-machine LynxKite image.
# Set --build-arg KITE_ENABLE_CUDA=yes to add CUDA dependencies.
ARG KITE_ENABLE_CUDA=no
FROM mambaorg/micromamba:1.5.8-jammy as cuda-no
FROM mambaorg/micromamba:1.5.8-jammy-cuda-12.4.1 as cuda-yes
FROM cuda-$KITE_ENABLE_CUDA
USER root

# Install dependencies early so they are in a low layer.
COPY lynxkite-env.yml /
RUN micromamba install -y -n base -f /lynxkite-env.yml && \
    micromamba clean --all --yes
COPY python_requirements.txt /
RUN /opt/conda/bin/pip install -r /python_requirements.txt

# CUDA dependencies.
ARG KITE_ENABLE_CUDA=no
ENV KITE_ENABLE_CUDA=$KITE_ENABLE_CUDA
COPY python_requirements_cuda.txt /
RUN if [ $KITE_ENABLE_CUDA = yes ]; then \
    /opt/conda/bin/pip install -r /python_requirements_cuda.txt; fi

# Install LynxKite.
COPY lynxkite.jar /
COPY run.sh /

# We add Conda to LD_LIBRARY_PATH to make NetworKit available.
# But then system binaries start using the wrong libtinfo.so.6.
RUN rm /opt/conda/lib/libtinfo.so.6
ENV LD_LIBRARY_PATH /opt/conda/lib
ENV ADD_TO_PYTHON_JAIL /opt/conda/lib

# The user has to configure these.
ENV KITE_MASTER_MEMORY_MB 1024
ENV SPHYNX_CACHED_ENTITIES_MAX_MEM_MB 1024
VOLUME /data
VOLUME /metadata

CMD ["/run.sh"]

EXPOSE 2200
