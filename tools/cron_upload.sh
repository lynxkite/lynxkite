#!/bin/bash
#
# Runs the bigraph uploadLogs, which will send the logs and operation performance data
# to LynxAnalytics.
#
# This script should be run as a cron job!


KITE_SITE_CONFIG=${KITE_SITE_CONFIG:-$HOME/.kiterc}
source ${KITE_SITE_CONFIG}

if [ -f "${KITE_SITE_CONFIG_OVERRIDES}" ]; then
  source ${KITE_SITE_CONFIG_OVERRIDES}
fi

COMMAND=`tail -n1 $KITE_SCRIPT_LOGS | cut -d ' ' -f3`

$COMMAND uploadLogs

