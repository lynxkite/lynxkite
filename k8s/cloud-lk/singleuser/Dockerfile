from jupyter/scipy-notebook:7a0c7325e470

user root
# Install stable dependencies.
run \
  apt-get update && \
  apt-get install -y busybox openjdk-8-jre nginx gettext-base curl && \
  busybox --install && \
  (curl -fsSL https://deb.nodesource.com/setup_14.x | bash -) && \
  apt-get install -y nodejs && \
  npm install -g typescript npx && \
  jupyter labextension install @jupyterlab/hub-extension && \
  pip install pycrypto ruamel.yaml

# Install Spark early, so the layer doesn't need to be pushed so often.
add build/stage/conf/SPARK_VERSION /tmp/build/stage/conf/SPARK_VERSION
add build/stage/tools/install_spark.sh /tmp/build/stage/tools/install_spark.sh
add build/stage/sphynx/python/install-dependencies.sh /tmp/build/stage/sphynx/python/install-dependencies.sh
run \
  cd /tmp/build/stage/sphynx/python/ && \
  ./install-dependencies.sh && \
  HOME=/tmp /tmp/build/stage/tools/install_spark.sh

# Set up frequently changing things.
add --chown=1000:100 build /tmp/build
add --chown=1000:100 patcher /tmp/patcher
run \
  pip install -e /tmp/build/python/remote_api && \
  pip install -e /tmp/build/stage/tools/graphray && \
  pip uninstall -y pyarrow && pip install pyarrow && \
  /tmp/patcher/prepare.sh
# sphynx/python/install-dependencies.sh has already installed PyArrow, but Conda decides on a super
# old version. Pip installs the newest, regardless of conflicts. It works!
add --chown=1000:100 preloaded_lk_data /tmp/preloaded_lk_data
add --chown=1000:100 kiterc /tmp/
add Welcome-template.ipynb /tmp/
add workspaces /tmp/workspaces
add lynx-start.sh /usr/local/bin/
add scripts /tmp/scripts
add nginx.conf /tmp/
run chmod -R o+w /tmp/ && echo "ALL ALL = (ALL) NOPASSWD: ALL" >> /etc/sudoers
env KITE_SITE_CONFIG /tmp/kiterc
user jovyan
cmd /usr/local/bin/lynx-start.sh
