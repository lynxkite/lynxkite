# This is to avoid this file being on CLASSPATH due to the $SPARK_CLASSPATH:... bug in
# compute-classpath.sh. We don't need this once https://issues.apache.org/jira/browse/SPARK-4831
# is resolved.
conf_dir="$(realpath "${app_home}/../conf")"
rm ${conf_dir}/play.plugins || true

# The folliwing inspired by the run() method of the original runner script. The point here is to
# setup java opts and application opts from the parameters of the runner script.
process_args "$@"
set -- "${residual_args[@]}"

# This is done here (I believe) because get_mem_ops uses java_opts.
if [[ "$JAVA_OPTS" != "" ]]; then
  java_opts="${JAVA_OPTS}"
fi

final_app_mem=${app_mem:-1024}

final_java_opts="$(get_mem_opts $final_app_mem) ${java_opts} ${java_args[@]}"

fake_application_jar=${lib_dir}/empty.jar

execRunner ${SPARK_HOME}/bin/spark-submit \
  --class play.core.server.NettyServer \
  --master ${SPARK_MASTER} \
  --driver-class-path "${app_classpath}" \
  --deploy-mode client \
  --driver-java-options "${final_java_opts}" \
  --driver-memory ${final_app_mem}m \
  ${fake_application_jar} \
  "${app_commands[@]}" \
  "${residual_args[@]}"

exit
