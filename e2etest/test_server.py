#!/usr/bin/env python
import httplib
import json
import os
import signal
import subprocess
import sys
import time
import traceback
import urllib

class PlayServer:
  def __init__(self, path, port):
    self.path = path
    self.port = port
    self.conn = httplib.HTTPConnection('localhost:' + port)
    self.serv = None
    logpath = path + '/logs/e2eplay.log'
    self.playlog = open(logpath, 'a')
    print 'Server stdout will be redirected to:', logpath

  def __enter__(self):
    return self

  def __exit__(self, exception_type, exception_value, traceback):
    if self.serv != None:
      os.killpg(self.serv.pid, signal.SIGTERM)
      self.serv.wait()
      print 'Server terminated'
    self.playlog.flush()

  def start(self, timeout=10, interval=2):
    self.serv = subprocess.Popen(['stage/bin/biggraph', '-Dhttp.port=%s' % self.port],
                                 cwd=self.path, stdout=self.playlog, preexec_fn=os.setsid)
    start = time.time()
    while (time.time() - start <= timeout):
      time.sleep(interval)
      try:
        self.conn.connect()
        print 'Connected to Play! Framework'
        return
      except:
        print 'Waiting for the server to start...'
    raise Exception('Connection timed out')

  def get_json_data(self, URI):
    print 'Sending request: [%s]' % URI
    self.conn.request('GET', URI)
    res = self.conn.getresponse()
    print res.status, res.reason
    assert (res.status == 200), 'HTTP GET failed'
    return json.loads(res.read())

def main(argv):
  port = '9005' if (len(argv) == 1) else argv[1]
  path = os.path.dirname(os.path.abspath(__file__)) + '/..'
  subprocess.check_call(['sbt', 'stage'], cwd=path)
  try:
    with PlayServer(path, port) as server:
      server.start()
      print 'Requesting initial biggraph metadata...'
      data = server.get_json_data('/ajax/graph?q={"id":"x"}')
      opid = -1
      for op in data['ops']:
        if op['name'] == 'Edge Graph':
          opid = op['operationId']
      assert opid >= 0, 'Edge Graph operation not found'
      request = (
        '/ajax/derive?' +
        urllib.urlencode({
          'q': ('{\n'
                '  "sourceIds": ["x"],\n'
                '  "operation": {\n'
                '    "operationId": %d,\n'
                '    "parameters": []\n'
                '  }\n'
                '}' % opid)}))
      basics = server.get_json_data(request)
      guid = basics["id"]
      print 'Requesting derived EdgeGraph data...'
      data = server.get_json_data('/ajax/stats?q={"id":"%s"}' % guid)
      assert (data['verticesCount'] == 4), 'Vertices count should be 4'
      assert (data['edgesCount'] == 4), 'Edges count should be 4'
      print 'Test succeeded'
  except Exception as e:
    print 'ERROR: Test failed with:', e.message
    traceback.print_exc()
    sys.exit(-1)

if __name__ == '__main__':
  main(sys.argv)

