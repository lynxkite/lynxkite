#!/bin/bash -ue
# Find the projects with big data files on PizzaKite.
# Run from the active metadata directory.

cd /data/partitioned
du --max-depth=1 . | sort -n | tail -n 100 > /tmp/bigfiles
cd - > /dev/null
cat /tmp/bigfiles | while read size guid; do
  cp=$(grep -l $guid checkpoints/* | tail -n 1 | sed 's/.*-//')
  if [ -n "$cp" ]; then
    project=$(grep "$cp" tags.journal | tail -n 1 | sed -n 's/.*projects\/\(.*\)\/\w*heckpoint.*/\1/p')
    echo "$size kb $guid $cp $project"
  fi
done
