#!/usr/bin/env python
"""Run this script from the root directory then go to http://localhost:8000/"""

import SimpleHTTPServer
import SocketServer
import urllib

class Handler(SimpleHTTPServer.SimpleHTTPRequestHandler):

  def translate_path(self, path):
    """Serve /ajax/ paths from public-test/ajax/, everything else from public/."""
    path = path.split('?', 1)[0]
    path = path.split('#', 1)[0]
    path = urllib.unquote(path)
    if path.startswith('/ajax/'):
      return 'public-test' + path
    else:
      return 'public' + path

  def end_headers(self):
    """Add a no-cache header to make testing easier."""
    self.send_header(
        'Cache-Control',
        'no-store, no-cache, must-revalidate, post-check=0, pre-check=0')
    SimpleHTTPServer.SimpleHTTPRequestHandler.end_headers(self)

class Server(SocketServer.TCPServer):
  allow_reuse_address = True

httpd = Server(("", 8000), Handler)
httpd.serve_forever()
