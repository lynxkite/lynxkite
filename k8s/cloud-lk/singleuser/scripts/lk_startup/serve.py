#!/usr/bin/env python3
'''
Serves a single file (first argument) in this directory (no matter the path in the url)
Second argument: the port
'''
from http.server import BaseHTTPRequestHandler, HTTPServer
from sys import argv
import json

filename = argv[1]
port = int(argv[2])


def read_page():
  with open(filename, 'r') as fin:
    return fin.read().encode('utf-8')


page = read_page()


class Server(BaseHTTPRequestHandler):
  def _set_headers(self):
    self.send_response(200)
    self.send_header('Content-type', 'text/html')
    self.end_headers()

  def do_HEAD(self):
    self._set_headers()

  def do_GET(self):
    self._set_headers()
    self.wfile.write(page)


httpd = HTTPServer(('', port), Server)
httpd.serve_forever()
