#!/bin/bash
# This is a wrapper for lynxkite/bin/lynxkite. Should be run inside the Docker container!

rm -f root/kite.pid root/sphynx.pid

# Configure kiterc settings.
export KITE_HTTP_PORT=${KITE_HTTP_PORT:-2200}
export SPARK_MASTER=yarn
export SPARK_HOME=/spark
export YARN_CONF_DIR=${YARN_CONF_DIR:-/hadoop_conf}
export KITE_META_DIR=${KITE_META_DIR:-/metadata}
export KITE_EXTRA_JARS=${KITE_EXTRA_JARS:-/extra_jars/*}
export KITE_PREFIX_DEFINITIONS=${KITE_PREFIX_DEFINITIONS:-/prefix_definitions.txt}
export KITE_INTERNAL_WATCHDOG_TIMEOUT_SECONDS=${KITE_INTERNAL_WATCHDOG_TIMEOUT_SECONDS:-1200}

# For authentication.
export KITE_USERS_FILE=${KITE_USERS_FILE:-/auth/kite_users}
export KITE_APPLICATION_SECRET=${KITE_APPLICATION_SECRET:-'<random>'}
export KITE_HTTPS_PORT=${KITE_HTTPS_PORT:-2201}
export KITE_HTTPS_KEYSTORE=${KITE_HTTPS_KEYSTORE:-/auth/tomcat.keystore}
export KITE_HTTPS_KEYSTORE_PWD=${KITE_HTTPS_KEYSTORE_PWD:-\$(cat /auth/keystore-password)}
export KITE_HTTP_ADDRESS=${KITE_HTTP_ADDRESS:-0.0.0.0}

export KITE_SITE_CONFIG=${KITE_SITE_CONFIG:-/lynxkite/conf/kiterc_template}

exec lynxkite/bin/lynxkite -Dlogger.resource=logger-docker.xml "$@"
