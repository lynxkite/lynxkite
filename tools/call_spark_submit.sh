# The following is inspired by the run() method of the original runner script. The point here is to
# setup java opts and application opts from the parameters of the runner script.
process_args "$@"
set -- "${residual_args[@]}"

# This is done here (I believe) because get_mem_opts uses java_opts.
if [[ "$JAVA_OPTS" != "" ]]; then
  java_opts="${JAVA_OPTS}"
fi

final_app_mem=${app_mem:-1024}

final_java_opts="$(get_mem_opts $final_app_mem) ${java_opts} ${java_args[@]}"

fake_application_jar=${lib_dir}/empty.jar

KITE_SITE_CONFIG=${KITE_SITE_CONFIG:-$HOME/.kiterc}

if [ -f ${KITE_SITE_CONFIG} ]; then
  echo "Loading configuration from: ${KITE_SITE_CONFIG}"
  export SPARK_VERSION=`cat ${lib_dir}/../conf/SPARK_VERSION`
  source ${KITE_SITE_CONFIG}
else
  echo "Warning, no Kite Site Config found at: ${KITE_SITE_CONFIG}"
  echo "Default location is $HOME/.kiterc, but you can override via the environment variable:"
  echo "KITE_SITE_CONFIG"
fi

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
