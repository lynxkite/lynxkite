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


LOGSPOL=eyJleHBpcmF0aW9uIjogIjIwNTAtMDYtMTZUMTE6MTE6MTFaIiwKICJjb25kaXRpb25zIjogWwogIHsiYWNsIjogInByb2plY3QtcHJpdmF0ZSIgfSwKICB7ImJ1Y2tldCI6ICJraXRlbG9ncy1sb2dzIn0sCiAgWyJzdGFydHMtd2l0aCIsICIka2V5IiwgIiJdCiAgXQp9Cg==
LOGSSIG=Ca5Dkg+fMnNfHSG+d5oTqLmQgiRLYvpQDCPdH/G8eGjRA+UZ/pxB6HanwukP8SGCgnakjoPJTRRHIe7Oc1BeEXAsNcL51bkLG2p7OopfcpkxVRDES6g8Wgz+ukDP94+2jr/LdR2Nw8x3Rkk91hyReQ73K+SAZAd2s9dO5blqGD0/LKEJKHe8QZGDbBRot9XWKygXRWcCvGNBIGNY1jxwaX9nvCP5HEqXkiPYHGZ6o+3aEDY3BTQGGpwK4jBtBNuKi6JgmkLvQGqP7iDw2pnKS1HxF5jXcUMoJb4Oc0XRDDn9UQxonT7w2X/EaJ2g4Z0yiZy8DWK81djPFkIz26/OEw==
OPSPOL=eyJleHBpcmF0aW9uIjogIjIwNTAtMDYtMTZUMTE6MTE6MTFaIiwKICJjb25kaXRpb25zIjogWwogIHsiYWNsIjogInByb2plY3QtcHJpdmF0ZSIgfSwKICB7ImJ1Y2tldCI6ICJraXRlbG9ncy1vcHMifSwKICBbInN0YXJ0cy13aXRoIiwgIiRrZXkiLCAiIl0KICBdCn0K
OPSSIG=PZFdErbTW8wzJWOYfcs5RWLIkrRd+BTdC/eKcLPNyjybzhJ4utYxNKve7PYT85XvEYLb30bLmRLBQhGK1h41spj0GC/g6SComqqiDUzR/NNVlEAi7WgPrSN48xjoBtNn6ucA7IWIZGIRIOOvrM1QSRCk91wjRMRVYN1cY9D124bTR+RNWvcdbG32A4nQ+ynx8i8Ccs5WwGzK8bpfJI9curgYVHUxbzYEIvSysTYTS1V+uve5A732JQYhJBYf8jkxoiWJ3naPR0KAM+xFG+/TBRopoAEZg+/KKditNWAE2ZJyqQKgE92LYjYVFQuSrFHRLuqzvrNB3X6FX/no0ia3iA==
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

