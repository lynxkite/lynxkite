#!/bin/bash -xue
# This is a wrapper for lynxkite/bin/lynxkite. Should be run inside the Docker container!

rm -f root/kite.pid root/sphynx.pid

# Configure kiterc settings.
cat > /kiterc_overrides <<EOF
export KITE_INSTANCE="$KITE_INSTANCE"
export KITE_HTTP_PORT=2200
export KITE_DATA_DIR=file:/data
export KITE_MASTER_MEMORY_MB=$KITE_MASTER_MEMORY_MB
export SPHYNX_CACHED_ENTITIES_MAX_MEM_MB="$SPHYNX_CACHED_ENTITIES_MAX_MEM_MB"
export NUM_CORES_PER_EXECUTOR=$NUM_CORES_PER_EXECUTOR
export SPARK_HOME=/spark
export KITE_META_DIR=/metadata
export KITE_EXTRA_JARS=/extra_jars/*
export KITE_PREFIX_DEFINITIONS=/prefix_definitions.txt
export KITE_INTERNAL_WATCHDOG_TIMEOUT_SECONDS=1200

export KITE_TITLE="$KITE_TITLE"
export KITE_TAGLINE="$KITE_TAGLINE"
export KITE_FRONTEND_CONFIG=${KITE_FRONTEND_CONFIG@Q}
export KITE_USERS_FILE=/kite_users
export KITE_APPLICATION_SECRET='<random>'
export KITE_HTTPS_PORT=2201
export KITE_HTTPS_KEYSTORE=/tomcat.keystore
export KITE_HTTPS_KEYSTORE_PWD="$KITE_INSTANCE"
export KITE_GOOGLE_CLIENT_SECRET="$KITE_GOOGLE_CLIENT_SECRET"
export KITE_GOOGLE_CLIENT_ID="$KITE_GOOGLE_CLIENT_ID"
export KITE_GOOGLE_HOSTED_DOMAIN="$KITE_GOOGLE_HOSTED_DOMAIN"
export KITE_GOOGLE_REQUIRED_SUFFIX="$KITE_GOOGLE_REQUIRED_SUFFIX"
export KITE_GOOGLE_PUBLIC_ACCESS="$KITE_GOOGLE_PUBLIC_ACCESS"
export KITE_GOOGLE_WIZARD_ONLY="$KITE_GOOGLE_WIZARD_ONLY"
export KITE_ACCESS_WITHOUT_LOGIN="$KITE_ACCESS_WITHOUT_LOGIN"
export KITE_HOME_WITHOUT_LOGIN="$KITE_HOME_WITHOUT_LOGIN"
export KITE_WIZARD_ONLY_WITHOUT_LOGIN="$KITE_WIZARD_ONLY_WITHOUT_LOGIN"
export KITE_HTTP_ADDRESS=0.0.0.0
export KITE_ALLOW_PYTHON="$KITE_ALLOW_PYTHON"
export SPHYNX_CHROOT_PYTHON="$SPHYNX_CHROOT_PYTHON"
EOF

if [ -n "${KITE_DISABLE_SPHYNX:-}" ]; then
  echo 'unset SPHYNX_HOST' >> /kiterc_overrides
  echo 'unset SPHYNX_PORT' >> /kiterc_overrides
fi

export KITE_SITE_CONFIG=/lynxkite/conf/kiterc_template
export KITE_SITE_CONFIG_OVERRIDES=/kiterc_overrides
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
