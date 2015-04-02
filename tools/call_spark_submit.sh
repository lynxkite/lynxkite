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

pushd ${lib_dir}/..
conf_dir=`pwd`/conf
log_dir=`pwd`/logs
mkdir -p ${log_dir}
tools_dir=`pwd`/tools
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
addJPropIfNonEmpty pidfile.path "${KITE_PID_FILE}"
addJPropIfNonEmpty http.netty.maxInitialLineLength 10000


# -mem flag overrides KITE_MASTER_MEMORY_MB and we use 1024 if neither is set.
final_app_mem=${app_mem:-${KITE_MASTER_MEMORY_MB:-1024}}

final_java_opts="$(get_mem_opts $final_app_mem) ${java_opts} ${java_args[@]}"

# Cannot do this earlier, as the wrapping script is written in a -e hostile way. :(
set -eo pipefail

export REPOSITORY_MODE=${REPOSITORY_MODE:-"static<$KITE_META_DIR,$KITE_DATA_DIR>"}

if [ -z "${NUM_CORES_PER_EXECUTOR}" ]; then
  echo "Please define NUM_CORES_PER_EXECUTOR in the kite config file ${KITE_SITE_CONFIG}."
  exit 1
fi

if [ "${SPARK_MASTER}" == "yarn-client" ]; then
  if [ -z "${YARN_NUM_EXECUTORS}" ]; then
    echo "Please define YARN_NUM_EXECUTORS in the kite config file ${KITE_SITE_CONFIG}."
    exit 1
  fi
  if [ -z "${YARN_CONF_DIR}" ]; then
    echo "Please define YARN_CONFIG_DIR in the kite config file ${KITE_SITE_CONFIG}."
    exit 1
  fi

  YARN_SETTINGS="--num-executors ${YARN_NUM_EXECUTORS} --executor-cores ${NUM_CORES_PER_EXECUTOR}"
fi

if [ "${SPARK_MASTER}" == "local" ]; then
 export SPARK_MASTER="${SPARK_MASTER}[${NUM_CORES_PER_EXECUTOR}]"
fi

if [ "${#residual_args[@]}" -ne 1 ]; then
  echo "Usage: $0 interactive|start|stop|restart"
  exit 1
fi

export KITE_SCHEDULER_POOLS_CONFIG="${conf_dir}/scheduler-pools.xml"

mode=${residual_args[0]}

command=(
    ${SPARK_HOME}/bin/spark-submit \
    --class play.core.server.NettyServer \
    --master ${SPARK_MASTER} \
    --driver-class-path "${app_classpath}" \
    --deploy-mode client \
    --driver-java-options "${final_java_opts}" \
    --driver-memory ${final_app_mem}m \
    ${YARN_SETTINGS} \
    "${fake_application_jar}" \
    "${app_commands[@]}"
)

startKite () {
  if [ -f "${KITE_PID_FILE}" ]; then
    echo "Kite is already running (or delete ${KITE_PID_FILE})"
    exit 1
  fi
  nohup "${command[@]}" > ${log_dir}/kite.stdout.$$ 2> ${log_dir}/kite.stderr.$$ &
  echo "Kite server successfully started."
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
      echo "Process $PID seems totally unkillable. Giving up."
      exit 1
    else
      rm -f "${PID_FILE}" || true
      echo "${SERVICE_NAME} successfully stopped."
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
          echo "Kite Watchdog is already running (or delete ${WATCHDOG_PID_FILE})"
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
      echo "Kite Watchdog successfully started."
  fi
}

stopWatchdog () {
  stopByPIDFile "${WATCHDOG_PID_FILE}" "Kite Watchdog"
}

case $mode in
  interactive)
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
esac

exit
