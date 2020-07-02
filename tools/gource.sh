#!/bin/bash -xue

set -o pipefail

gitfaces .. faces

git log \
  --pretty=format:user:%an%n%ct \
  --reverse \
  --raw \
  --encoding=UTF-8 \
  --no-renames \
  > gource-git
sed -i 's/borsijulcsi/Julia Borsi/g' gource-git
sed -i 's/borsim2/Miklós Borsi/g' gource-git
sed -i 's/dolphyvn/Dolphy/g' gource-git
sed -i 's/erbenpeter/Erben Péter/g' gource-git
sed -i 's/Forevian/András Barják/g' gource-git
sed -i 's/gaborfeher/Gábor Fehér/g' gource-git
sed -i 's/Gabor Feher/Gábor Fehér/g' gource-git
sed -i 's/:Gabor$/:Gabor Olah/g' gource-git
sed -i 's/gsvigruha/Gergely Svigruha/g' gource-git
sed -i 's/hannagabor/Hanna Gábor/g' gource-git
sed -i 's/Hanna Gabor/Hanna Gábor/g' gource-git
sed -i 's/hiuy/Yenonn Hiu/g' gource-git
sed -i 's/malnapolya/Malna Polya/g' gource-git
sed -i 's/huncros/David Herskovics/g' gource-git
sed -i 's/J..nos Maginecz/Janos Maginecz/g' gource-git
sed -i 's/olahg/Gabor Olah/g' gource-git
sed -i 's/shuheng-lynx/Shu Heng/g' gource-git
sed -i 's/Steven XuFan/Steven Xu Fan/g' gource-git
sed -i 's/Svigruha Gergely/Gergely Svigruha/g' gource-git
sed -i 's/szmate1618/Mate Szabo/g' gource-git
sed -i 's/vincent/Chien-Hsun (Vincent) Chang/g' gource-git
sed -i 's/wafle/Janos Maginecz/g' gource-git
sed -i 's/xandrew-lynx/Andras Nemeth/g' gource-git
sed -i 's/zskatona/Zsolt Katona/g' gource-git
sed -i 's/ddkatona/Daniel Katona/g' gource-git

xvfb-run -a ~/Gource/gource \
  -1280x720 -r 60 \
  --path gource-git \
  --seconds-per-day 0.05 \
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
  --user-image-dir faces \
  -o - | \
ffmpeg \
  -y \
  -r 60 \
  -f image2pipe \
  -vcodec ppm \
  -i - \
  -vcodec libx264 \
  -preset medium \
  -tune animation \
  gource.mp4
