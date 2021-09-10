#!/bin/bash -xue
# Starts up LynxKite on a remote machine with Docker.

cd $(dirname $0)/../..

INSTANCE="$1"
export CONTINENT=${CONTINENT:-asia}
export REGION=${REGION:-asia-east1}
export ZONE=${ZONE:-asia-east1-b}
export PROJECT=${PROJECT:-big-graph-gc1}
export KITE_MASTER_MEMORY_MB=${KITE_MASTER_MEMORY_MB:-30000}
export NUM_CORES_PER_EXECUTOR=${NUM_CORES_PER_EXECUTOR:-*}
export KITE_HOSTNAME=${KITE_HOSTNAME:-$INSTANCE.lynxanalytics.com}

docker/lynxkite/local-letsencrypt/build.sh
gcloud config set project $PROJECT
IMAGE="${CONTINENT}.gcr.io/$PROJECT/lynx-kite_letsencrypt"
docker tag lynx/kite_letsencrypt:latest "$IMAGE"
gcloud auth configure-docker
>&2 docker push "$IMAGE"

SSH="gcloud compute ssh $INSTANCE --zone $ZONE --command"
USER="kite"
if [ "$INSTANCE" == "pizzakite" ]; then
  USER="darabos" # TODO: Remove when we rebuild PizzaKite.
fi
SU="sudo -H -u $USER"

if [[ $KITE_ALLOW_PYTHON = 'yes' ]]; then
  PYTHON_SETTINGS=" \
    -e KITE_ALLOW_PYTHON=yes \
    -e SPHYNX_CHROOT_PYTHON=yes \
    --privileged \
  "
fi

$SSH "$SU gcloud auth print-access-token | \
      $SU docker login -u oauth2accesstoken --password-stdin https://${CONTINENT}.gcr.io"
$SSH "$SU docker pull -- $IMAGE"
$SSH "$SU docker rm -f lynxkite || true"
KITE_FRONTEND_CONFIG="${KITE_FRONTEND_CONFIG:-}"
$SSH "$SU docker run \
  -d \
  --name lynxkite \
  --restart=always \
  --log-driver=gcplogs \
  -e KITE_MASTER_MEMORY_MB=$KITE_MASTER_MEMORY_MB \
  -e SPHYNX_CACHED_ENTITIES_MAX_MEM_MB=${SPHYNX_CACHED_ENTITIES_MAX_MEM_MB:-1500} \
  -e NUM_CORES_PER_EXECUTOR=$NUM_CORES_PER_EXECUTOR \
  -e KITE_INSTANCE=$INSTANCE \
  -e KITE_TITLE=\"$KITE_TITLE\" \
  -e KITE_TAGLINE=\"$KITE_TAGLINE\" \
  -e KITE_FRONTEND_CONFIG=${KITE_FRONTEND_CONFIG@Q} \
  -e KITE_DATA_COLLECTION=always \
  -e KITE_GOOGLE_CLIENT_SECRET=\"${KITE_GOOGLE_CLIENT_SECRET:-}\" \
  -e KITE_GOOGLE_CLIENT_ID=\"${KITE_GOOGLE_CLIENT_ID:-}\" \
  -e KITE_GOOGLE_HOSTED_DOMAIN=\"${KITE_GOOGLE_HOSTED_DOMAIN:-}\" \
  -e KITE_GOOGLE_REQUIRED_SUFFIX=\"${KITE_GOOGLE_REQUIRED_SUFFIX:-}\" \
  -e KITE_GOOGLE_PUBLIC_ACCESS=\"${KITE_GOOGLE_PUBLIC_ACCESS:-}\" \
  -e KITE_GOOGLE_WIZARD_ONLY=\"${KITE_GOOGLE_WIZARD_ONLY:-}\" \
  -e KITE_ACCESS_WITHOUT_LOGIN=\"${KITE_ACCESS_WITHOUT_LOGIN:-}\" \
  -e KITE_HOME_WITHOUT_LOGIN=\"${KITE_HOME_WITHOUT_LOGIN:-}\" \
  -e KITE_WIZARD_ONLY_WITHOUT_LOGIN=\"${KITE_WIZARD_ONLY_WITHOUT_LOGIN:-}\" \
  -e KITE_HOSTNAME=\"$KITE_HOSTNAME\" \
  -e KITE_AUTH_TRUSTED_PUBLIC_KEY_BASE64=\"${KITE_AUTH_TRUSTED_PUBLIC_KEY_BASE64:-}\" \
  -e KITE_DISABLE_SPHYNX=\"${KITE_DISABLE_SPHYNX:-}\" \
  ${PYTHON_SETTINGS:-} \
  -p 80:2200 \
  -p 443:2201 \
  -p 4040:4040 \
  -v ~$USER/kite/meta:/metadata \
  -v ~$USER/kite/data:/data \
  -v ~$USER/kite/sphynx/ordered:/root/ordered_sphynx_data \
  -v ~$USER/kite/sphynx/unordered:/root/unordered_sphynx_data \
  -v ~$USER/kite/static:/lynxkite/static \
  -v ~$USER/kite_users:/kite_users \
  -v /etc/letsencrypt:/letsencrypt \
  $IMAGE"

gcloud compute scp --zone $ZONE \
  tools/demokite/renewcert.sh $INSTANCE:
$SSH "sudo mv renewcert.sh ~$USER/"
$SSH "$SU bash -c \"echo '12 5,17 * * *' ~/renewcert.sh $KITE_HOSTNAME | crontab\""
