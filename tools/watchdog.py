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
import socket
import sys
import logging

flags = argparse.ArgumentParser(
    description='Runs a script if a URL is not responsive or when requested through a web UI.')
flags.add_argument('--status_port', type=int,
                   help='Port for the web UI.', required=True)


def splitURLRecord(record):
  url, timeout = record.split('@')
  return (url, int(timeout))
flags.add_argument('--watched_urls', type=lambda s: [splitURLRecord(r) for r in s.split(',')],
                   help='URLs to watch. A comma separated list of URL@timeout pairs.', required=True)
flags.add_argument('--sleep_seconds', type=int,
                   help='Time between health checks.', required=True)
flags.add_argument('--max_failures', type=int,
                   help='Number of failures before the script is run.', required=True)
flags.add_argument('--script',
                   help='Script to run when the port is unresponsive.', required=True)
flags.add_argument('--pid_file',
                   help='Where to put the PID file for this watchdof.', required=True)
flags = flags.parse_args()


logging.basicConfig(
    destination=sys.stderr,
    level=logging.DEBUG,
    format="%(asctime)s  %(message)s",
    datefmt="%d/%h/%Y %H:%M:%S")


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

  def log_message(self, format, *args):
    client = self.client_address[0]
    msg = "client: " + self.client_address[0] + " -- " + format % args
    logging.info(msg)


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
    logging.error('Failed %d time(s)', self.worry)
    if self.worry == flags.max_failures and self.enabled:
      logging.error('Failure %d reached, I will restart', flags.max_failures)
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
  '''Returns True if all watched URLs returns HTTP status 200 within the specified timeout.'''
  try:
    for (url, timeout) in flags.watched_urls:
      urllib2.urlopen(url, timeout=timeout)
  except urllib2.URLError:
    return False
  except socket.timeout:
    return False
  return True


if __name__ == '__main__':
  pidfile = flags.pid_file
  if os.path.isfile(pidfile):
    print "%s already exists, exiting" % pidfile
    sys.exit()
  else:
    with file(pidfile, 'w') as f:
      f.write(str(os.getpid()))

  server = Server()
  thread.start_new_thread(monitor_thread, (server,))
  server.serve_forever()
