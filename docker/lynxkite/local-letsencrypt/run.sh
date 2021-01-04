#!/bin/bash -xue
# This is a wrapper for lynxkite/bin/lynxkite. Should be run inside the Docker container!

rm -f root/kite.pid root/sphynx.pid

# Configure kiterc settings.
export KITE_HTTP_PORT=${KITE_HTTP_PORT:-2200}
export KITE_DATA_DIR=${KITE_DATA_DIR:-file:/data}
export SPARK_HOME=/spark
export KITE_META_DIR=${KITE_META_DIR:-/metadata}
export KITE_EXTRA_JARS=${KITE_EXTRA_JARS:-/extra_jars/*}
export KITE_PREFIX_DEFINITIONS=${KITE_PREFIX_DEFINITIONS:-/prefix_definitions.txt}
export KITE_INTERNAL_WATCHDOG_TIMEOUT_SECONDS=${KITE_INTERNAL_WATCHDOG_TIMEOUT_SECONDS:-1200}

export KITE_FRONTEND_CONFIG=${KITE_FRONTEND_CONFIG@Q}
export KITE_USERS_FILE=${KITE_USERS_FILE:-/kite_users}
export KITE_APPLICATION_SECRET=${KITE_APPLICATION_SECRET:-'<random>'}
export KITE_HTTPS_PORT=${KITE_HTTPS_PORT:-2201}
export KITE_HTTPS_KEYSTORE=${KITE_HTTPS_KEYSTORE:-/tomcat.keystore}
export KITE_HTTPS_KEYSTORE_PWD=${KITE_HTTPS_KEYSTORE_PWD:-"$KITE_INSTANCE"}
export KITE_HTTP_ADDRESS=${KITE_HTTP_ADDRESS:-0.0.0.0}

if [ -n "${KITE_DISABLE_SPHYNX:-}" ]; then
  export SPHYNX_HOST=""
  export SPHYNX_PORT=""
fi

export KITE_SITE_CONFIG=${KITE_SITE_CONFIG:-/lynxkite/conf/kiterc_template}
export KITE_HOSTNAME=${KITE_HOSTNAME:-$KITE_INSTANCE.lynxanalytics.com}

cd letsencrypt/live/$KITE_HOSTNAME
openssl \
  pkcs12 \
  -export \
  -in fullchain.pem \
  -inkey privkey.pem \
  -out /tomcat.keystore \
  -name tomcat \
  -CAfile chain.pem \
  -caname root \
  -password pass:$KITE_INSTANCE
cd -

exec lynxkite/bin/lynxkite -Dlogger.resource=logger-docker.xml "$@"
