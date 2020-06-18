#!/bin/bash -xue
# Upload all workspaces from /tmp/workspaces/
# We wait for LK to be available, upload the workspaces
# and then notify nginx that LK is ready.

/tmp/scripts/wait_for_port.sh 2200

export LYNXKITE_ADDRESS="https://localhost:9345"
export LYNXKITE_PUBLIC_SSL_CERT="/home/jovyan/.credentials/lk-private.crt"

python3 -c "import lynx.kite; lk = lynx.kite.LynxKite(); lk.remove_name('Tutorials', force=True)"

UW=/tmp/scripts/upload_workspace.py
WS=/tmp/workspaces/

${UW} --folder Tutorials --ws_name "1. [Beginner] Understand airport significance" --ws_file ${WS}/Flight_Routes.yaml

sed -i 's/localhost:8000/localhost:2200/g' /tmp/nginx.final.conf
sed -i '/max-age=0/d' /tmp/nginx.final.conf
# Make nginx reload the config
kill -HUP `cat /tmp/nginx.pid`

# We don't need the dummy server to run anymore
kill `cat /tmp/startup_page.pid`
rm /tmp/startup_page.pid
