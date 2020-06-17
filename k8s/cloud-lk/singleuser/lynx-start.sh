#!/bin/bash -xue

echo 'making credentials'
# Generate keypair for LynxKite authentication.
mkdir -p .credentials
cd .credentials
openssl genrsa -out lk-private.pem 2048
openssl rsa -in lk-private.pem -outform DER -out lk-private.der
openssl rsa -in lk-private.pem -outform DER -pubout -out lk-public.der
openssl req -new -key lk-private.pem -out lk-private.csr -subj "/C=HU/ST=Budapest/L=Budapest/O=Nothing/OU=Dep/CN=localhost"
openssl x509 -req -days 10000 -in lk-private.csr -signkey lk-private.pem -out lk-private.crt

export KITE_AUTH_TRUSTED_PUBLIC_KEY_BASE64=$(cat lk-public.der | base64 -w0)
export LYNXKITE_SIGNED_TOKEN=$(python /tmp/scripts/sign_token.py)
export LYNXKITE_SIGNED_TOKEN_URL=$(python -c 'import urllib.parse, os; print(urllib.parse.quote(os.environ["LYNXKITE_SIGNED_TOKEN"]))')
export LYNXKITE_ADDRESS="https://demo.lynxkite.com${JUPYTERHUB_SERVICE_PREFIX}lk"
cd -

echo 'envsubst'
# We cannot put files in /home/jovyan/ in Dockerfile because it will be replaced by a volume mount.
envsubst < /tmp/Welcome-template.ipynb > Welcome.ipynb

echo 'Tutorials'
mkdir -p 'Tutorials'
for f in /tmp/build/python/documentation/*.{png,ipynb}; do
  cp -R "$f" 'Tutorials'
done

echo 'copy from preloaded'
mkdir -p .kite/data
mkdir -p .kite/meta/1/operations

cp -R /tmp/preloaded_lk_data/data .kite/
cp -R /tmp/preloaded_lk_data/meta/1/operations .kite/meta/1/

echo 'starting the surrogate LK page'
/tmp/scripts/lk_startup/serve.py /tmp/scripts/lk_startup/index.html 8000 &
echo "$!" > /tmp/startup_page.pid

echo 'starting lynxkite'
/tmp/scripts/delete_multiple_ops.py --folder /home/jovyan/.kite/meta/1/operations
cd /tmp
rm -rf /tmp/kite.pid /tmp/sphynx.pid
# Start LynxKite and Nginx in the background with authentication
/tmp/build/stage/bin/lynxkite -Dlogger.resource=logger-docker.xml interactive &

cd -
echo 'sed nginx.conf'
sed s,/lk/,${JUPYTERHUB_SERVICE_PREFIX}lk/, < /tmp/nginx.conf > /tmp/nginx.final.conf
nginx -c /tmp/nginx.final.conf -p . > /tmp/nginx_output 2>&1 &

echo 'applying patch'
# Apply JupyterLab patch.
/tmp/patcher/patch.sh

/tmp/scripts/upload_workspaces.sh &

# Back to the regular start command.
if [ "${TESTRUN:-unset}" = "unset" ]; then
    echo 'starting labhub'
    exec jupyter-labhub "$@" --port=8889
else
    echo 'testrun'
    /tmp/scripts/wait_for_port.sh 2200
    while [ -f /tmp/startup_page.pid ];
    do
        sleep 1
    done
    echo "KITE_APPLICATION_SECRET=" >> /tmp/kiterc
    kill `cat /tmp/kite.pid`
    /tmp/build/stage/bin/lynxkite interactive
fi
