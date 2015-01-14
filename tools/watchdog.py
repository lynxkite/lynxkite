#!/usr/bin/env python
import argparse
import BaseHTTPServer
import socket
import subprocess
import thread
import time
import urllib2
import os
import signal
import sys

flags = argparse.ArgumentParser(
  description='Runs a script if a URL is not responsive or when requested through a web UI.')
flags.add_argument('--status_port', type=int,
    help='Port for the web UI.', required=True)
flags.add_argument('--watched_url',
    help='URL to watch.', required=True)
flags.add_argument('--sleep_seconds', type=int,
    help='Time between health checks.', required=True)
flags.add_argument('--max_failures', type=int,
    help='Number of failures before the script is run.', required=True)
flags.add_argument('--script',
    help='Script to run when the port is unresponsive.', required=True)
flags.add_argument('--pid_file',
    help='Where to put the PID file for this watchdof.', required=True)
flags = flags.parse_args()


class Handler(BaseHTTPServer.BaseHTTPRequestHandler):

  def do_GET(self):
    '''Status page with log and controls.'''
    self.send_response(200)
    self.send_header('Content-type', 'text/html')
    self.end_headers()
    log = '<pre>' + self.server.snippet() + '</pre>'
    restart = '<form method="POST" action="/restart"><button>Restart</button></form>'
    if self.server.enabled:
      toggle = '<form method="POST" action="/disable"><button>Disable</button></form>'
    else:
      toggle = '<form method="POST" action="/enable"><button>Enable</button></form>'
    self.wfile.write('<html>' + log + restart + toggle + '</html>')

  def do_POST(self):
    print self.path
    if self.path == '/restart':
      self.server.restart()
    elif self.path == '/enable':
      self.server.enabled = True
    elif self.path == '/disable':
      self.server.enabled = False
    else:
      assert False, 'Unexpected request: ' + self.path
    self.send_response(301)
    self.send_header('Location', '/')
    self.end_headers()


class Server(BaseHTTPServer.HTTPServer):

  def __init__(self):
    self.log = []  # Health check history.
    self.worry = 0  # Number of consecutive failures in the last period.
    self.enabled = True
    BaseHTTPServer.HTTPServer.__init__(self, ('', flags.status_port), Handler)

  def server_bind(self):
    '''Allow re-using the port.'''
    self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    BaseHTTPServer.HTTPServer.server_bind(self)

  def snippet(self):
    '''Returns a string that encodes the results of the last 100 health checks.'''
    return ''.join(self.log[-100:])

  def log_success(self):
    self.log.append('.')
    self.worry = 0

  def log_failure(self):
    self.log.append('X')
    self.worry += 1
    if self.worry == flags.max_failures and self.enabled:
      self.restart()

  def restart(self):
    self.log.append('!')
    subprocess.call(
      flags.script,
      shell=True,
      preexec_fn=lambda: signal.signal(signal.SIGPIPE, signal.SIG_DFL))


def monitor_thread(server):
  '''Thread for periodic health checks. Results are logged in the server.'''
  while True:
    if health_check():
      server.log_success()
    else:
      server.log_failure()
    time.sleep(flags.sleep_seconds)


def health_check():
  '''Returns True if the URL returns HTTP status 200 within a second.'''
  try:
    urllib2.urlopen(flags.watched_url, timeout=1)
  except urllib2.URLError:
    return False
  return True


if __name__ == '__main__':
  pidfile = flags.pid_file
  if os.path.isfile(pidfile):
    print "%s already exists, exiting" % pidfile
    sys.exit()
  else:
    file(pidfile, 'w').write(str(os.getpid()))
  
  server = Server()
  thread.start_new_thread(monitor_thread, (server,))
  server.serve_forever()
