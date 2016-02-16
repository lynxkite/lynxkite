#!/bin/bash

set -eu
set -o pipefail

function asciidoc_to_template {
  python << END |          # Comment out text except code blocks.
f = open('$1', 'r')
codeBlock = False
for line in f.readlines():
  if line == '\`\`\`\n':
    codeBlock = not codeBlock
  else:
    if codeBlock:
      print line,
    else:
      if line == '\n':
        print line,
      else:
        print '# ' + line,
END
  sed -e '/\[\[.*\]\]/d' | # Remove achors. \
  sed -e '/\#\#\#.*/d' |   # Removes headers. \
  sed -e 's/`//g' > $2     # Remove backquotes.
}

APP_HOME=$(dirname $(dirname $(readlink -f $0)))

asciidoc_to_template \
  $APP_HOME/web/app/admin_manual/installation/configuration/kiterc.asciidoc \
  $APP_HOME/conf/kiterc_template

