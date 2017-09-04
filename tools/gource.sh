#!/bin/bash -xue

set -o pipefail

git log \
  --pretty=format:user:%ae%n%ct \
  --reverse \
  --raw \
  --encoding=UTF-8 \
  --no-renames \
  > /tmp/gource$$

gource \
  --path /tmp/gource$$ \
  --seconds-per-day 0.01 \
  --log-format git \
  --hide filenames \
  --camera-mode overview \
  --highlight-dirs \
  --dir-name-depth 3 \
  --highlight-users \
  --key \
  --padding 1.5 \
  --stop-at-end \
  --max-user-speed 1000 \
  -o - \
  | avconv \
  -y \
  -r 60 \
  -f image2pipe \
  -vcodec ppm \
  -i - \
  -vcodec libx264 \
  -preset ultrafast \
  -tune animation \
  gource.mp4

rm -f /tmp/gource$$
