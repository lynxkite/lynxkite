#!/bin/bash -xua
# This script starts postgres and then
# supervisor

/etc/init.d/postgresql start
/usr/local/bin/supervisord -c /tmp/supervisord.conf

