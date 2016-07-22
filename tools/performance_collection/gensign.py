#!/usr/bin/env python
'''
Generates a script that can upload kite logs and the extracted operation performance data
to the google storage. Only needs to be run when changes are made to the *.policy
files in this directory. Run it like this:

python gensign.py > gupload.sh

The new version of gupload.sh should then be committed

'''


from M2Crypto import EVP
from os.path import expanduser
from base64 import b64encode
import sys

policy_files = ['kitelogs-logs.policy', 'kitelogs-ops.policy']


def b64_file(filename):
  with open(filename, 'r') as file:
    data = file.read()
    return b64encode(data)

encoded_policies = map(b64_file, policy_files)


def generate_signature(encoded_policy):
  pemfile_name = expanduser('~') + '/.ssh/kite-logs.pem'
  with open(pemfile_name, 'r') as pemfile:
    pem_key = pemfile.read()
    key = EVP.load_key_string(pem_key)
    key.reset_context(md='sha256')
    key.sign_init()
    key.sign_update(str(encoded_policy))
    return b64encode(key.sign_final())

signatures = map(generate_signature, encoded_policies)

print '''#!/bin/bash
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

'''

print 'LOGSPOL=' + encoded_policies[0]
print 'LOGSSIG=' + signatures[0]
print 'OPSPOL=' + encoded_policies[1]
print 'OPSSIG=' + signatures[1]
print 'LOGSBUCKET=kitelogs-logs'
print 'OPSBUCKET=kitelogs-ops'

print '''
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


curl \\
$TRACE \\
--form policy=$POL \\
--form signature=$SIG \\
--form acl=project-private \\
--form key=$OUTPUT \\
--form GoogleAccessId=kite-logs-upload@big-graph-gc1.iam.gserviceaccount.com \\
--form file=@$INPUT \\
http://${BUCKET}.storage.googleapis.com
'''
