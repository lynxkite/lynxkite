'''Python interface for the named pipe-based LynxKite API.

Reads the pipe location from .kiterc just like LynxKite does.

Example usage:

    import lynx
    p = lynx.Project()
    p.newVertexSet(size=100)
    print(p.scalar('vertex_count'))

The list of operations is not documented, but you can copy the invocation from a LynxKite project
history.
'''
import atexit
import json
import os
import re
import subprocess
import tempfile
import types


def _init():
  '''Runs when the module is loaded. Reads configuration and creates response pipe.'''
  global server_pipe, response_pipe
  server_pipe = _fromkiterc('KITE_API_PIPE')
  response_pipe = '{}/pipe-{}'.format(tempfile.gettempdir(), os.getpid())
  os.mkfifo(response_pipe)
  atexit.register(_cleanup)


def _cleanup():
  '''Runs at exit to remove the response pipe.'''
  os.remove(response_pipe)


class Project(object):
  '''Represents an unanchored LynxKite project.

  This project is not automatically saved to the LynxKite project directories.
  '''
  def __init__(self):
    '''Creates a new blank project.'''
    r = _send('newProject')
    self.checkpoint = r.checkpoint

  def scalar(self, scalar):
    '''Fetches the value of a scalar. Returns either a double or a string.'''
    r = _send('getScalar', dict(checkpoint=self.checkpoint, scalar=scalar))
    if hasattr(r, 'double'):
      return r.double
    return r.string

  def run_operation(self, operation, parameters):
    '''Runs an operation on the project with the given parameters.'''
    r = _send('runOperation',
        dict(checkpoint=self.checkpoint, operation=operation, parameters=parameters))
    self.checkpoint = r.checkpoint

  def __getattr__(self, attr):
    '''For any unknown names we return a function that tries to run an operation by that name.'''
    def f(**kwargs):
      params = {}
      for k, v in kwargs.items():
        params[k] = str(v)
      self.run_operation(attr, params)
    return f


class LynxException(Exception):
  '''Raised when LynxKite indicates that an error has occured while processing a command.'''
  def __init__(self, error, command):
    super(LynxException, self).__init__(error)
    self.error = error
    self.command = command


def _send(command, payload={}):
  '''Sends a command to LynxKite and returns the response when it arrives.'''
  msg = json.dumps(dict(command=command, payload=payload, responsePipe=response_pipe))
  with open(server_pipe, 'w') as p:
    p.write(msg)
  with open(response_pipe) as p:
    data = p.read()
    r = json.loads(data, object_hook=_asobject)
    if hasattr(r, 'error'):
      raise LynxException(r.error, r.request)
    return r


def _asobject(dic):
  '''Wraps the dict in a namespace for easier access. I.e. d["x"] becomes d.x.'''
  return types.SimpleNamespace(**dic)


def _fromkiterc(variable):
  '''Returns the value of a variable defined in .kiterc.'''
  load_kiterc = '''
      KITE_SITE_CONFIG=${KITE_SITE_CONFIG:-$HOME/.kiterc}
      source "$KITE_SITE_CONFIG"
      if [ -f "$KITE_SITE_CONFIG_OVERRIDES" ]; then
        source "$KITE_SITE_CONFIG_OVERRIDES"
      fi
      '''
  # We will source kiterc, then print the variable we want.
  command = '{}; echo ${}'.format(load_kiterc, variable)
  return subprocess.check_output(command, shell=True, executable='/bin/bash').strip()


_init()
