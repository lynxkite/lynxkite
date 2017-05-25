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
log_dir=${KITE_LOG_DIR:-${stage_dir}/logs}
mkdir -p ${log_dir}
tools_dir=${stage_dir}/tools
popd > /dev/null


export SPARK_VERSION=`cat ${conf_dir}/SPARK_VERSION`
export KITE_DEPLOYMENT_CONFIG_DIR=${conf_dir}
export KITE_STAGE_DIR=${stage_dir}
export KITE_LOG_DIR=${log_dir}
if [ -f ${KITE_SITE_CONFIG} ]; then
  >&2 echo "Loading configuration from: ${KITE_SITE_CONFIG}"
  source ${KITE_SITE_CONFIG}
else
  >&2 echo "Warning, no LynxKite Site Config found at: ${KITE_SITE_CONFIG}"
  >&2 echo "Default location is $HOME/.kiterc, but you can override via the environment variable:"
  >&2 echo "KITE_SITE_CONFIG"
  >&2 echo "You can find an example config file at ${conf_dir}/kiterc_template"
fi

if [ -f "${KITE_SITE_CONFIG_OVERRIDES}" ]; then
  >&2 echo "Loading configuration overrides from: ${KITE_SITE_CONFIG_OVERRIDES}"
  source ${KITE_SITE_CONFIG_OVERRIDES}
fi

randomString () {
    echo `cat /dev/urandom | od -x --read-bytes=16  --address-radix=n | tr -d " \n"`
}

if [ -n "$KITE_APPLICATION_SECRET" ]; then
  if [ "$KITE_APPLICATION_SECRET" == "<random>" ]; then
    KITE_APPLICATION_SECRET=$(randomString)
  fi
  # SECRET(secret_string) is converted to *** when logged.
  KITE_APPLICATION_SECRET="SECRET(${KITE_APPLICATION_SECRET})"
fi


addJPropIfNonEmpty () {
  if [ -n "$2" ]; then
    addJava "-D$1=$2"
  fi
}

addJPropIfNonEmpty lynxkite.spark_home ${SPARK_HOME}
addJPropIfNonEmpty java.security.policy ${conf_dir}/security.policy
addJPropIfNonEmpty http.port "${KITE_HTTP_PORT}"
addJPropIfNonEmpty https.port "${KITE_HTTPS_PORT}"
addJPropIfNonEmpty https.keyStore "${KITE_HTTPS_KEYSTORE}"
addJPropIfNonEmpty https.keyStorePassword "${KITE_HTTPS_KEYSTORE_PWD}"
addJPropIfNonEmpty application.secret "${KITE_APPLICATION_SECRET}"
addJPropIfNonEmpty authentication.google.clientSecret "${KITE_GOOGLE_CLIENT_SECRET}"
addJPropIfNonEmpty hadoop.tmp.dir "${KITE_LOCAL_TMP}"
addJPropIfNonEmpty pidfile.path "/dev/null"
addJPropIfNonEmpty http.netty.maxInitialLineLength 10000
addJPropIfNonEmpty jdk.tls.ephemeralDHKeySize 2048
addJPropIfNonEmpty file.encoding 'UTF-8'

mode=${residual_args[0]}

if [ "$mode" == "batch" ]; then
  # We set up log config to use Play's stupid non-default log config file location.
  addJPropIfNonEmpty logback.configurationFile "${conf_dir}/logger.xml"
  addJPropIfNonEmpty application.home "${stage_dir}"
fi

# -mem flag overrides KITE_MASTER_MEMORY_MB and we use 1024 if neither is set.
final_app_mem=${app_mem:-${KITE_MASTER_MEMORY_MB:-1024}}
final_java_opts="${java_opts} ${java_args[@]}"

# Cannot do this earlier, as the wrapping script is written in a -e hostile way. :(
set -eo pipefail

export REPOSITORY_MODE="static<$KITE_META_DIR,$KITE_DATA_DIR,$KITE_EPHEMERAL_DATA_DIR>"

if [ -z "${NUM_CORES_PER_EXECUTOR}" ]; then
  >&2 echo "Please define NUM_CORES_PER_EXECUTOR in the kite config file ${KITE_SITE_CONFIG}."
  exit 1
fi

if [ "${SPARK_MASTER}" == "yarn-client" ]; then
  >&2 echo \
    'SPARK_MASTER=yarn-client is deprecated in Spark 2.x. Please change to >SPARK_MASTER=yarn.'
  SPARK_MASTER=yarn
fi

if [ "${SPARK_MASTER}" == "yarn" ]; then
  if [ -z "${NUM_EXECUTORS}" ]; then
    >&2 echo "Please define NUM_EXECUTORS in the kite config file ${KITE_SITE_CONFIG}."
    exit 1
  fi
  if [ -z "${YARN_CONF_DIR}" ]; then
    >&2 echo "Please define YARN_CONF_DIR in the kite config file ${KITE_SITE_CONFIG}."
    exit 1
  fi

  # TODO: we may not actually need this as we set spark.executor.cores in BiggraphSparkContext.
  YARN_SETTINGS="--executor-cores ${NUM_CORES_PER_EXECUTOR}"

  # Override memory overhead.
  if [ -n "${YARN_EXECUTOR_MEMORY_OVERHEAD_MB}" ]; then
    COMPUTED_EXECUTOR_MEMORY_OVERHEAD_MB="${YARN_EXECUTOR_MEMORY_OVERHEAD_MB}"
  else
    RATIO_PERCENT=20
    LAST_CHAR=${EXECUTOR_MEMORY: -1}
    if [ "${LAST_CHAR}" == "m" ]; then
      COMPUTED_EXECUTOR_MEMORY_OVERHEAD_MB=$((${EXECUTOR_MEMORY%?} * $RATIO_PERCENT / 100))
    elif [ "${LAST_CHAR}" == "g" ]; then
      COMPUTED_EXECUTOR_MEMORY_OVERHEAD_MB=$((${EXECUTOR_MEMORY%?} * 1024 * $RATIO_PERCENT / 100))
    else
      <&2 echo "Cannot parse: EXECUTOR_MEMORY=${EXECUTOR_MEMORY}. Should be NNNg or NNNm"
      exit 1
    fi
  fi
  YARN_SETTINGS="$YARN_SETTINGS \
    --conf spark.yarn.executor.memoryOverhead=${COMPUTED_EXECUTOR_MEMORY_OVERHEAD_MB}"
fi

if [ -n "${NUM_EXECUTORS}" ]; then
  if [ "${SPARK_MASTER}" == "yarn" ]; then
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

if [ -n "${RESOURCE_POOL}" ]; then
  if [ "${SPARK_MASTER}" == "yarn" ]; then
    RESOURCE_POOL_OPTION="--queue ${RESOURCE_POOL}"
  else
     >&2 echo "Resource pool option is only supported for master: yarn"
     exit 1
  fi
else
    RESOURCE_POOL_OPTION=""
fi

if [ -n "${KERBEROS_PRINCIPAL}" ] || [ -n "${KERBEROS_KEYTAB}" ]; then
  if [ -z "${KERBEROS_PRINCIPAL}" ] || [ -z "${KERBEROS_KEYTAB}" ]; then
    >&2 echo "Please define KERBEROS_PRINICPAL and KERBEROS_KEYTAB together: either both of them or none."
    exit 1
  fi
  EXTRA_OPTIONS="${EXTRA_OPTIONS} --principal ${KERBEROS_PRINCIPAL} --keytab ${KERBEROS_KEYTAB}"
fi

if [ "${SPARK_MASTER}" == "local" ]; then
 export SPARK_MASTER="${SPARK_MASTER}[${NUM_CORES_PER_EXECUTOR}]"
fi

FULL_CLASSPATH=${app_classpath}
if [ -n "${KITE_EXTRA_JARS}" ]; then
    EXPANDED_EXTRA_JARS=$(python -c \
        "import glob; print(':'.join(sum([glob.glob(p) for p in '${KITE_EXTRA_JARS}'.split(':')], [])))")
    if [ -n "$EXPANDED_EXTRA_JARS" ]; then
      FULL_CLASSPATH=${FULL_CLASSPATH}:${EXPANDED_EXTRA_JARS}
    fi
fi

if [ "${mode}" == "batch" ]; then
  className="com.lynxanalytics.biggraph.BatchMain"
else
  className="play.core.server.NettyServer"
fi

SPARK_JARS_REPLACE_FROM=":/"
SPARK_JARS_REPLACE_TO=",file:/"
# This list will become the spark.jars Spark property. (Unless it is overwritten later
# in SparkConfig.)
SPARK_JARS="file:"${FULL_CLASSPATH//$SPARK_JARS_REPLACE_FROM/$SPARK_JARS_REPLACE_TO}

command=(
    ${SPARK_HOME}/bin/spark-submit \
    --class "${className}" \
    --master "${SPARK_MASTER}" \
    --driver-class-path "${FULL_CLASSPATH}" \
    --deploy-mode client \
    --driver-java-options "${final_java_opts}" \
    --driver-memory ${final_app_mem}m \
    --jars "${SPARK_JARS}" \
    ${EXTRA_OPTIONS} \
    ${YARN_SETTINGS} \
    ${DEV_EXTRA_SPARK_OPTIONS} \
    ${RESOURCE_POOL_OPTION} \
    "${fake_application_jar}" \
    "${app_commands[@]}" \
    "${residual_args[@]:1}"
)

startKite () {
  if [ ! -d "${SPARK_HOME}" ]; then
    >&2 echo "Spark cannot be found at ${SPARK_HOME}"
    exit 1
  fi
  export KITE_READY_PIPE=/tmp/kite_pipe_$(randomString)
  mkfifo ${KITE_READY_PIPE}
  nohup "${command[@]}" > ${log_dir}/kite.stdout.$$ 2> ${log_dir}/kite.stderr.$$ &
  PID=$!
  read RESULT < ${KITE_READY_PIPE}
  STATUS=`echo $RESULT | cut -f 1 -d " "`
  if [[ "${STATUS}" == "ready" ]]; then
    >&2 echo "LynxKite server started (PID ${PID})."
  else
    >&2 echo "LynxKite server failed: $RESULT"
    exit 1
  fi
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
  stopByPIDFile ${KITE_PID_FILE} "LynxKite server"
}

WATCHDOG_PID_FILE="${KITE_PID_FILE}.watchdog"
startWatchdog () {
  if [ -n "${KITE_WATCHDOG_PORT}" ]; then
      if [ -f "${WATCHDOG_PID_FILE}" ]; then
          >&2 echo "LynxKite Watchdog is already running (or delete ${WATCHDOG_PID_FILE})"
          exit 1
      fi
      MAIN_URL="http://localhost:${KITE_HTTP_PORT}/"
      SPARK_CHECK_URL="${MAIN_URL}sparkHealthCheck"
      nohup ${tools_dir}/watchdog.py \
          --status_port=${KITE_WATCHDOG_PORT} \
          --watched_urls="${MAIN_URL}@1,${SPARK_CHECK_URL}@120" \
          --sleep_seconds=60 \
          --max_failures=6 \
          --script="$0 watchdog_restart" \
          --pid_file ${WATCHDOG_PID_FILE} \
          > ${log_dir}/watchdog.stdout.$$ 2> ${log_dir}/watchdog.stderr.$$ &
      >&2 echo "LynxKite Watchdog started (PID $!)."
  fi
}

stopWatchdog () {
  stopByPIDFile "${WATCHDOG_PID_FILE}" "LynxKite Watchdog"
}

uploadLogs () {
  if [ -z ${KITE_INSTANCE} ]; then
    >&2 echo "KITE_INSTANCE is not set. Cannot upload logs."
    exit 1
  fi
  THIS_DIR="$(dirname "$(readlink -f "$0")")"
  UPLOADER=${THIS_DIR}/../tools/performance_collection/multi_upload.sh
  if [ -f "${KITE_PID_FILE}" ]; then
      ${UPLOADER} ${KITE_HTTP_PORT} ${KITE_LOG_DIR} ${KITE_INSTANCE}
  else
      ${UPLOADER} 0                 ${KITE_LOG_DIR} ${KITE_INSTANCE}
  fi
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
  uploadLogs)
    uploadLogs
  ;;
  *)
    >&2 echo "Usage: $0 interactive|start|stop|restart|batch|uploadLogs"
    exit 1
  ;;
esac

if [ -n "${KITE_SCRIPT_LOGS}" ]; then
    THIS_PROG="$(readlink -f "$0")"
    NOW=`date "+%Y:%m:%d %H:%M:%S"`
    echo $NOW $THIS_PROG $mode >> $KITE_SCRIPT_LOGS
fi

exit
