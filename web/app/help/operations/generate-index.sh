#!/bin/sh -ue

cd $(dirname $0)

sed '/Autogenerated content below this line./q' index.asciidoc \
  > index.asciidoc~generated

LC_ALL=C ls *.asciidoc \
  | grep 'asciidoc' \
  | grep -v 'index.asciidoc' \
  | grep -v 'glossary.asciidoc' \
  | sed 's/\(.*\)/\ninclude::\1[]/' \
  >> index.asciidoc~generated

mv index.asciidoc~generated index.asciidoc
