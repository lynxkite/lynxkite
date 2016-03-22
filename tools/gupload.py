#!/usr/bin/env python

# Script to upload files to the google data store.
# Example usages:
#
# 1. Uploads application-20160316.log to gs://kite-logs/full-logs/pizzakite/application-20160316.log:
# gupload.py --input application-20160316.log  --output full-logs/pizzakite/application-20160316.log
#
# 2. Generates a new signature (only needed when the policy is changed):
# gupload.py --generate_signature
#



import argparse
import sys
from base64 import b64encode
import subprocess

flags = argparse.ArgumentParser(description='Uploads a file to google data store')
 
flags.add_argument('--input', help='The name of the input file to upload')
flags.add_argument('--output', help='The name of the output file to be created in the bucket')
flags.add_argument('--generate_signature', help='Dump a new signature',
                   action='store_true')
flags.add_argument('--trace_file', help='If given, dumps curl communication to this file')

flags = flags.parse_args()


policy = '''
{"expiration": "2050-06-16T11:11:11Z",
 "conditions": [
  {"acl": "bucket-owner-read" },
  {"bucket": "kite-logs"},
  ["starts-with", "$key", ""]
  ]
}
'''
# If you change the policy, you'll need to use the --generate_signature option and replace
# the value of hardwired_signature with the output.
hardwired_signature = 'drwUkULtYBJ4LIEVHK8d/TuzZcrHLgnJdhRcJk4dFuVh1qOrW+VGms3BchR7PLBd5va+N9dsyASZ/LBe5EuQDdrKzkfTq29ztzO74KgbVDCWQqU+dK//6Rkc2p0DNnceEaeAFtfgO9IiapRFYac7i4OQFo/Zv/XnCrYJ0a8ChgBa01fRBl0AqAXWlMvkaXQKBCPQItxw7VwVvH1qTcddVrUQRnpR2rw3HTgbtlENyw633lXt01KkD19haWAcXKtBZLumNCgLhvFHUP8hyqV9iWymeRsObyUkwy6JhZTsWN/nWOvaVlq7uRMD5wlmfVjwKJcn6nHMZSlNFeABmqqjkQ=='

encoded_policy = b64encode(policy)

def generate_signature():
    from M2Crypto import EVP
    from os.path import expanduser

    pemfile = expanduser("~") + '/.ssh/kite-logs.pem'
    pem_key = open(pemfile, 'r').read()
    assert pem_key    
    key = EVP.load_key_string(pem_key)
    key.reset_context(md='sha256')
    key.sign_init()
    key.sign_update(str(encoded_policy))
    return b64encode(key.sign_final())


if flags.generate_signature:
    print generate_signature()
else:
    if not flags.input:
        sys.stderr.write('Please, specify an input file (--input)\n')
        sys.exit (1)
    
    if not flags.output:
        sys.stderr.write('Please, specify an output file (--output)\n')
        sys.exit (1)
    
    cmd = ['curl']
    if flags.trace_file :
        cmd.extend(['--trace-ascii', flags.trace_file])
    cmd.extend(['--form', 'policy='+encoded_policy])
    cmd.extend(['--form', 'signature='+hardwired_signature])
    cmd.extend(['--form', 'acl=bucket-owner-read'])
    cmd.extend(['--form', 'key='+flags.output])
    cmd.extend(['--form', 'GoogleAccessId=kite-logs-upload@big-graph-gc1.iam.gserviceaccount.com'])
    cmd.extend(['--form',  'file=@'+flags.input])
    cmd.append('http://kite-logs.storage.googleapis.com')
    subprocess.call(cmd)

