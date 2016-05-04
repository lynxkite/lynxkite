#!/bin/bash

set -xueo pipefail

# Wait till kite can actually serve requests
sleep 5 



# Dynamic content is escaped in error messages
curl -v "http://localhost:${PORT}/<script>cross_site_scripting.nasl</script>.asp" | grep "&lt;script&gt;cross_site_scripting.nasl&lt;"



# Logfiles get rotated
LOGFILES1=`ls stage/logs | wc -l`
EXPECTED=$((LOGFILES1 + 1))
curl -d '{"fake": 0}' -H "Content-Type: application/json" "http://localhost:${PORT}/forceLogRotate"
sleep 3
LOGFILES2=`ls stage/logs | wc -l`
if [ "$EXPECTED" != "$LOGFILES2" ]; then
    exit 1
fi



