#!/bin/sh -xue

# I have no idea why, but if I remove "ls", nohup does not work.
CMD="$@"
sh -c "( ( nohup $CMD &> detached_run.$$.out ) & ls > /dev/null )"
