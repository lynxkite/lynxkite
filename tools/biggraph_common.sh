set -ueo pipefail
function trap_handler() {
  THIS_SCRIPT="$0"
  LAST_LINENO="$1"
  LAST_COMMAND="$2"
  echo "${THIS_SCRIPT}:${LAST_LINENO}: error while executing: ${LAST_COMMAND}" 1>&2;
}
trap 'trap_handler ${LINENO} "${BASH_COMMAND}"' ERR

