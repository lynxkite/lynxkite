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

pushd ${lib_dir}/.. > /dev/null
stage_dir=`pwd`
conf_dir=${stage_dir}/conf
log_dir=${stage_dir}/logs
mkdir -p ${log_dir}
tools_dir=${stage_dir}/tools
popd > /dev/null


export SPARK_VERSION=`cat ${conf_dir}/SPARK_VERSION`
export KITE_RANDOM_SECRET=$(python -c \
  'import random, string; print "".join(random.choice(string.letters) for i in range(32))')
export KITE_DEPLOYMENT_CONFIG_DIR=${conf_dir}
export KITE_STAGE_DIR=${stage_dir}
if [ -f ${KITE_SITE_CONFIG} ]; then
  >&2 echo "Loading configuration from: ${KITE_SITE_CONFIG}"
  source ${KITE_SITE_CONFIG}
else
  >&2 echo "Warning, no Kite Site Config found at: ${KITE_SITE_CONFIG}"
  >&2 echo "Default location is $HOME/.kiterc, but you can override via the environment variable:"
  >&2 echo "KITE_SITE_CONFIG"
  >&2 echo "You can find an example config file at ${conf_dir}/kiterc_template"
fi

if [ -f "${KITE_SITE_CONFIG_OVERRIDES}" ]; then
  >&2 echo "Loading configuration overrides from: ${KITE_SITE_CONFIG_OVERRIDES}"
  source ${KITE_SITE_CONFIG_OVERRIDES}
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
addJPropIfNonEmpty pidfile.path "${KITE_PID_FILE}"
addJPropIfNonEmpty http.netty.maxInitialLineLength 10000

mode=${residual_args[0]}

if [ "$mode" == "batch" ]; then
  # We set up log config to use Play's stupid non-default log config file location.
  addJPropIfNonEmpty logback.configurationFile "${conf_dir}/logger.xml"
  addJPropIfNonEmpty application.home "${stage_dir}"
fi

# -mem flag overrides KITE_MASTER_MEMORY_MB and we use 1024 if neither is set.
final_app_mem=${app_mem:-${KITE_MASTER_MEMORY_MB:-1024}}

final_java_opts="$(get_mem_opts $final_app_mem) ${java_opts} ${java_args[@]}"

# Cannot do this earlier, as the wrapping script is written in a -e hostile way. :(
set -eo pipefail

export REPOSITORY_MODE="static<$KITE_META_DIR,$KITE_DATA_DIR,$KITE_EPHEMERAL_DATA_DIR>"

if [ -z "${NUM_CORES_PER_EXECUTOR}" ]; then
  >&2 echo "Please define NUM_CORES_PER_EXECUTOR in the kite config file ${KITE_SITE_CONFIG}."
  exit 1
fi

if [ "${SPARK_MASTER}" == "yarn-client" ]; then
  if [ -z "${NUM_EXECUTORS}" ]; then
    >&2 echo "Please define NUM_EXECUTORS in the kite config file ${KITE_SITE_CONFIG}."
    exit 1
  fi
  if [ -z "${YARN_CONF_DIR}" ]; then
    >&2 echo "Please define YARN_CONFIG_DIR in the kite config file ${KITE_SITE_CONFIG}."
    exit 1
  fi

  # TODO: we may not actually need this as we set spark.executor.cores in BiggraphSparkContext.
  YARN_SETTINGS="--executor-cores ${NUM_CORES_PER_EXECUTOR}"
fi

if [ -n "${NUM_EXECUTORS}" ]; then
  if [ "${SPARK_MASTER}" == "yarn-client" ]; then
    # YARN mode
    EXTRA_OPTIONS="${EXTRA_OPTIONS} --num-executors ${NUM_EXECUTORS}"
  elif [[ "${SPARK_MASTER}" == spark* ]]; then
    # Standalone mode
    TOTAL_CORES=$((NUM_EXECUTORS * NUM_CORES_PER_EXECUTOR))
    EXTRA_OPTIONS="${EXTRA_OPTIONS} --total-executor-cores ${TOTAL_CORES}"
  else
    >&2 echo "Num executors is not supported for master: ${SPARK_MASTER}"
    exit 1
  fi
fi

if [ "${SPARK_MASTER}" == "local" ]; then
 export SPARK_MASTER="${SPARK_MASTER}[${NUM_CORES_PER_EXECUTOR}]"
fi

export KITE_SCHEDULER_POOLS_CONFIG="${conf_dir}/scheduler-pools.xml"

FULL_CLASSPATH=${app_classpath}
if [ -n "${KITE_EXTRA_JARS}" ]; then
  FULL_CLASSPATH=${FULL_CLASSPATH}:${KITE_EXTRA_JARS}
fi

if [ "${mode}" == "batch" ]; then
  className="com.lynxanalytics.biggraph.BatchMain"
else
  className="play.core.server.NettyServer"
fi

command=(
    ${SPARK_HOME}/bin/spark-submit \
    --class "${className}" \
    --master "${SPARK_MASTER}" \
    --driver-class-path "${FULL_CLASSPATH}" \
    --deploy-mode client \
    --driver-java-options "${final_java_opts}" \
    --driver-memory ${final_app_mem}m \
    ${EXTRA_OPTIONS} \
    ${YARN_SETTINGS} \
    "${fake_application_jar}" \
    "${app_commands[@]}" \
    "${residual_args[@]:1}"
)

startKite () {
  if [ -f "${KITE_PID_FILE}" ]; then
    >&2 echo "Kite is already running (or delete ${KITE_PID_FILE})"
    exit 1
  fi
  if [ ! -d "${SPARK_HOME}" ]; then
    >&2 echo "Spark cannot be found at ${SPARK_HOME}"
    exit 1
  fi
  nohup "${command[@]}" > ${log_dir}/kite.stdout.$$ 2> ${log_dir}/kite.stderr.$$ &
  >&2 echo "Kite server started (PID $!)."
}

stopByPIDFile () {
  PID_FILE=$1
  SERVICE_NAME=$2
  if [ -f "${PID_FILE}" ]; then
    PID=$(cat "${PID_FILE}")
    kill $PID || true
    for i in $(seq 10); do
      if [ ! -e /proc/$PID ]; then
        break
      fi
      sleep 1
    done
    if [ -e /proc/$PID ]; then
      kill -9 $PID || true
      sleep 1
    fi
    if [ -e /proc/$PID ]; then
      >&2 echo "Process $PID seems totally unkillable. Giving up."
      exit 1
    else
      rm -f "${PID_FILE}" || true
      >&2 echo "${SERVICE_NAME} successfully stopped."
    fi
  fi
}

stopKite () {
  stopByPIDFile ${KITE_PID_FILE} "Kite server"
}

WATCHDOG_PID_FILE="${KITE_PID_FILE}.watchdog"
startWatchdog () {
  if [ -n "${KITE_WATCHDOG_PORT}" ]; then
      if [ -f "${WATCHDOG_PID_FILE}" ]; then
          >&2 echo "Kite Watchdog is already running (or delete ${WATCHDOG_PID_FILE})"
          exit 1
      fi
      MAIN_URL="http://localhost:${KITE_HTTP_PORT}/"
      SPARK_CHECK_URL="${MAIN_URL}sparkHealthCheck"
      nohup ${tools_dir}/watchdog.py \
          --status_port=${KITE_WATCHDOG_PORT} \
          --watched_urls="${MAIN_URL}@1,${SPARK_CHECK_URL}@120" \
          --sleep_seconds=10 \
          --max_failures=10 \
          --script="$0 watchdog_restart" \
          --pid_file ${WATCHDOG_PID_FILE} \
          > ${log_dir}/watchdog.stdout.$$ 2> ${log_dir}/watchdog.stderr.$$ &
      >&2 echo "Kite Watchdog started (PID $!)."
  fi
}

stopWatchdog () {
  stopByPIDFile "${WATCHDOG_PID_FILE}" "Kite Watchdog"
}

case $mode in
  interactive)
    exec "${command[@]}"
  ;;
  batch)
    exec "${command[@]}"
  ;;
  start)
    startKite
    startWatchdog
  ;;
  stop)
    stopWatchdog
    stopKite
  ;;
  restart)
    stopWatchdog
    stopKite
    startKite
    startWatchdog
  ;;
  watchdog_restart)
    stopKite
    startKite
  ;;
  *)
    >&2 echo "Usage: $0 interactive|start|stop|restart|batch"
    exit 1
  ;;
esac

exit
