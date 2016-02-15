#!/bin/bash

set -eu
set -o pipefail

function asciidoc_to_template {
  sed -e '/\[\[.*\]\]/d' $1 | \
  awk 'BEGIN {
    codeBlock = false;
  }
  {
    if ($0 == "```") {
      codeBlock = !codeBlock
    } else {
      if (codeBlock) {
        print $0
      } else {
        if ($0 == "") {
          print $0
        } else {
          print "# " $0
        }
      }
    }
  }' | \
  sed -e 's/`//g' > $2
}

APP_HOME=$(dirname $(dirname $(readlink -f $0)))

asciidoc_to_template $APP_HOME/web/app/admin_manual/installation/bash_templates/kiterc.asciidoc $APP_HOME/conf/kiterc_template

