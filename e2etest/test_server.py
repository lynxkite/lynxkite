#!/usr/bin/env python
import httplib
import json
import os
import signal
import subprocess
import sys
import time
import traceback

class PlayServer:
  def __init__(self, path, port):
    self.path = path
    self.conn = httplib.HTTPConnection('localhost:' + port)
    logpath = path + '/logs/e2eplay.log'
    self.playlog = open(logpath, 'a')
    print('Server stdout will be redirected to:'), logpath

  def __enter__(self):
    return self
    
  def __exit__(self, exception_type, exception_value, traceback):
    os.killpg(self.serv.pid, signal.SIGTERM)
    self.serv.wait()
    print('Server terminated')
    self.playlog.flush()

  def start(self, timeout=10, debug=True, interval=2):
    cmd = 'run' if debug else 'start'
    self.serv = subprocess.Popen(['sbt', cmd], cwd=path, stdout=self.playlog, preexec_fn=os.setsid)
    start = time.time()
    while (time.time() - start <= timeout):  
      time.sleep(interval)
      try:
        self.conn.connect()
        print('Connected to Play! Framework')
        return
      except:
        print('Waiting for the server to start...')
    raise Exception('Connection timed out')
    
  def get_json_data(self, URI):
    self.conn.request('GET', URI)
    res = self.conn.getresponse()
    print res.status, res.reason
    assert (res.status == 200), 'HTTP GET failed'
    return json.loads(res.read())

port = '9000' if (len(sys.argv) == 1) else sys.argv[1]
path = os.path.dirname(os.path.abspath(__file__)) + '/..'
subprocess.Popen(['sbt', 'package'], cwd=path).wait()
try:
  with PlayServer(path, port) as server:
    server.start()
    print('Requesting initial biggraph metadata...')
    data = server.get_json_data('/ajax/graph?q={"id":"x"}')
    guid = data['ops'][0]['id']
    print guid
    print('Requesting derived EdgeGraph data...')
    data = server.get_json_data('/ajax/stats?q={"id":"%s"}' % guid)
    assert (data['vertices_count'] == 4), 'Vertices count should be 4'
    print('Test succeeded')
except Exception as e:
  print('ERROR: Test failed with:'), e.message
  traceback.print_exc()

