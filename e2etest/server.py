#!/usr/bin/env python

import httplib
import subprocess
import time
import sys
import json

subprocess.Popen(['sbt', 'package'], cwd='..').wait()
playlog = open('play.log', 'a')
serv = subprocess.Popen(['sbt', 'run'], cwd='..', stdout=playlog)
start = time.time()
conn = httplib.HTTPConnection('localhost:9000')
connected = False
while (time.time() - start <= 10):  
  time.sleep(2)
  try:
    conn.connect()
    print('Connected to Play! Framework')
    connected = True
    break;
  except:
    print('Waiting for the server to start...')
if connected == True:
  print('Requesting initial biggraph metadata...')
  conn.request('GET','/ajax/graph?q={"id":"x"}')
  res = conn.getresponse()
  print res.status, res.reason
  if res.reason == 'OK':
    data = json.loads(res.read())
    guid = data['ops'][0]['id']
    print guid
    print('Requesting derived EdgeGraph data...')
    conn.request('GET','/ajax/stats?q={"id":"%s"}' % guid)
    res = conn.getresponse()
    print res.status, res.reason
    data = json.loads(res.read())
    result = data['vertices_count']
    if result == 4:
      print('Test succeeded')
    else:
      print('ERROR: Test failed')
    print('hit ctrl-D to terminate server, see play logs in play.log')
  serv.wait()
  playlog.flush()
else:
  print "Connection timed out"

