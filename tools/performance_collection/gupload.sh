
#!/bin/bash
#
# GENERATED FILE, DO NOT EDIT!!!
# Use gensign.py to generate this whenever you make a change to the *.policy files.
#
# This script uploads logs and operation performance data to the google storage.
#
# Logs are uploaded to gs://kitelogs-logs (a bucket in the east asia region)
# Operation performance data are uploaded to gs://kitelogs-ops/ (a bucket in the US region)
# You can choose between the two by specifying either log or ops as the first
# command line parameter.
#
# Usage: gupload.sh log|ops file-to-upload target-file [optional trace file]
#
# Examples:
#
# 1. From pizzakite, upload an application log. Here, the target file should
#    also contain the directory of the instance (in this case, pizzakite):
#
#    gupload.sh  log  application-20160201.log pizzakite/application-20160201.log
#
#
# 2. Upload the operation logs extracted from the application log (by parse_log.sh).
#    Suppose the extracted operation logs are in file oplogs-20160201.txt. Such
#    oplogs will all go to the same directory (putting them in subdirectories would
#    make querying impossible), so we name the target file pizzakite-oplogs-20160201.txt
#    to make sure it is different from oplogs extracted from other instances.
#
#    gupload.sh  ops  oplogs-20160201.txt pizzakite-oplogs-20160201.txt
#
#
# 3. The same as above, but create a trace file (trace.txt) that contains all the
#    http communication between our program and the google servers.
#
#    gupload.sh  ops  oplogs-20160201.txt pizzakite-oplogs-20160201.txt trace.txt
#
#


LOGSPOL=eyJleHBpcmF0aW9uIjogIjIwNTAtMDYtMTZUMTE6MTE6MTFaIiwKICJjb25kaXRpb25zIjogWwogIHsiYWNsIjogImJ1Y2tldC1vd25lci1yZWFkIiB9LAogIHsiYnVja2V0IjogImtpdGVsb2dzLWxvZ3MifSwKICBbInN0YXJ0cy13aXRoIiwgIiRrZXkiLCAiIl0KICBdCn0K
LOGSSIG=czXyRf8ATDGLD+3lR/SKXtIe9W2cTaYxIu5A+7R6z7WJHoBNZ7FMXNfrE2GorxsOBhCW/lZ3Vq5PXZJPHCkViNieVJZBL8L+qw9OwXtG0Gpg0jUu4QsOY86sfH/0kyWGI43ubw2cn2CVaq3+Ef2T9xjyMPzuFgWTvDGzJEE8oV4Sad0BcYNQWLIjDimb5KPYF5i7A7KdSkge5MLcqbXeVgkAIEgsAzdKpON6yXaEWunfKCaCP0c0GsAu+Jq/DguflY49Aofv4LAU1Yx+QIpnLPYLuNrfU71NkQBBSI9UhKp9jJr7xXx9r13xIpuYaiUtekHPFUxboggOubRIz0S8ow==
OPSPOL=eyJleHBpcmF0aW9uIjogIjIwNTAtMDYtMTZUMTE6MTE6MTFaIiwKICJjb25kaXRpb25zIjogWwogIHsiYWNsIjogImJ1Y2tldC1vd25lci1yZWFkIiB9LAogIHsiYnVja2V0IjogImtpdGVsb2dzLW9wcyJ9LAogIFsic3RhcnRzLXdpdGgiLCAiJGtleSIsICIiXQogIF0KfQo=
OPSSIG=WkcBbOENhdRN5XTqawlNFY/UwDNkYf/tjjzdQjZwiA9i5d9Hm8jqYJan7U2SMNa43HMUAp/v1F5vcOLr9WTqH4/TqP0+YC9GGcvfFeSiBbuuZmd/dFrFip6PAnVsXqtZJD85pVMKcTUpK/ifJl1yQjYLKpzrKvj/DQzKRg567Tb3u5uvuptS64sIOJkI6sMnhIDxWC7qHCPytfnpwdaSfON3vmeiMgXch9mlI9h+Xm4f1TyC13ddnxPFr+k6v2t1eHhUvDsqluZSlxQsuwLhYrAIrIfdzVZ4+0rDGjWwFoXw/EfcVVxaSDUDzVP7baoFsjFEQZ/cumw7n3mr1UneEA==
LOGSBUCKET=kitelogs-logs
OPSBUCKET=kitelogs-ops

if [ "$#" -lt 3 ]; then
    echo "Usage: $0 log|ops file-to-upload target-file [optional trace file]" 1>&2
    exit 1
fi

INPUT=$2

if [ ! -f "$INPUT" ]; then
  echo "$INPUT does not exist." 1>&2
  exit 1
fi

OUTPUT=$3

case $1 in
log)
        POL=$LOGSPOL
        SIG=$LOGSSIG
        BUCKET=$LOGSBUCKET
;;
ops)
        POL=$OPSPOL
        SIG=$OPSSIG
        BUCKET=$OPSBUCKET
;;
*)
        echo "First argument must be either log or ops" 1>&2
        exit 1
;;
esac

if [ -z "$4" ]; then
    TRACE=
else
    TRACE="--trace-ascii $4"
fi


curl \
$TRACE \
--form policy=$POL \
--form signature=$SIG \
--form acl=bucket-owner-read \
--form key=$OUTPUT \
--form GoogleAccessId=kite-logs-upload@big-graph-gc1.iam.gserviceaccount.com \
--form file=@$INPUT \
http://${BUCKET}.storage.googleapis.com

