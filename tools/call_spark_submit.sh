# The following is inspired by the run() method of the original runner script. The point here is to
# setup java opts and application opts from the parameters of the runner script.
process_args "$@"
set -- "${residual_args[@]}"

# This is done here (I believe) because get_mem_opts uses java_opts.
if [[ "$JAVA_OPTS" != "" ]]; then
  java_opts="${JAVA_OPTS}"
fi

fake_application_jar=${lib_dir}/empty.jar

KITE_SITE_CONFIG=${KITE_SITE_CONFIG:-$HOME/.kiterc}

pushd ${lib_dir}/../conf
conf_dir=`pwd`
popd

if [ -f ${KITE_SITE_CONFIG} ]; then
  echo "Loading configuration from: ${KITE_SITE_CONFIG}"
  export SPARK_VERSION=`cat ${conf_dir}/SPARK_VERSION`
  export KITE_RANDOM_SECRET=`cat /dev/urandom | tr -dc 'a-zA-Z0-9' | fold -w 32 | head -n 1`
  export KITE_DEPLOYMENT_CONFIG_DIR=${conf_dir}
  source ${KITE_SITE_CONFIG}
else
  echo "Warning, no Kite Site Config found at: ${KITE_SITE_CONFIG}"
  echo "Default location is $HOME/.kiterc, but you can override via the environment variable:"
  echo "KITE_SITE_CONFIG"
  echo "You can find an example config file at ${conf_dir}/kiterc_template"
fi

addJPropIfNonEmpty () {
  if [ -n "$2" ]; then
    addJava "-D$1=$2"
  fi
}

addJPropIfNonEmpty http.port "${KITE_HTTP_PORT}"
addJPropIfNonEmpty https.port "${KITE_HTTPS_PORT}"
addJPropIfNonEmpty https.keyStore "${KITE_HTTPS_KEYSTORE}"
addJPropIfNonEmpty https.keyStorePassword "${KITE_HTTPS_KEYSTORE_PWD}"
addJPropIfNonEmpty application.secret "${KITE_APPLICATION_SECRET}"
addJPropIfNonEmpty authentication.google.clientSecret "${KITE_GOOGLE_CLIENT_SECRET}"
addJPropIfNonEmpty hadoop.tmp.dir "${KITE_LOCAL_TMP}"


# -mem flag overrides KITE_MASTER_MEMORY_MB and we use 1024 if neither is set.
final_app_mem=${app_mem:-${KITE_MASTER_MEMORY_MB:-1024}}

final_java_opts="$(get_mem_opts $final_app_mem) ${java_opts} ${java_args[@]}"

export REPOSITORY_MODE=${REPOSITORY_MODE:-"static<$KITE_META_DIR,$KITE_DATA_DIR>"}

execRunner ${SPARK_HOME}/bin/spark-submit \
  --class play.core.server.NettyServer \
  --master ${SPARK_MASTER} \
  --driver-class-path "${app_classpath}" \
  --deploy-mode client \
  --driver-java-options "${final_java_opts}" \
  --driver-memory ${final_app_mem}m \
  "${fake_application_jar}" \
  "${app_commands[@]}" \
  "${residual_args[@]}"

exit
