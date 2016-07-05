'''Python interface for the LynxKite Remote API.

The access to the LynxKite instance can be configured through the following environment variables:

    LYNXKITE_ADDRESS=https://lynxkite.example.com/
    LYNXKITE_USERNAME=user@company
    LYNXKITE_PASSWORD=my_password

Example usage:

    import lynx
    p = lynx.Project()
    p.newVertexSet(size=100)
    print(p.scalar('vertex_count'))

The list of operations is not documented, but you can copy the invocation from a LynxKite project
history.
'''
import http.cookiejar
import json
import os
import types
import urllib


default_sql_limit = 1000


def reconnect():
  '''Runs when the module is loaded. Performs login.'''
  global connection
  cj = http.cookiejar.CookieJar()
  connection = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cj))
  if os.environ.get('LYNXKITE_USERNAME'):
    _request('/passwordLogin', dict(
        username=os.environ['LYNXKITE_USERNAME'],
        password=os.environ['LYNXKITE_PASSWORD']))


class Project(object):
  '''Represents an unanchored LynxKite project.

  This project is not automatically saved to the LynxKite project directories.
  '''
  @staticmethod
  def load(name):
    p = Project()
    r = _send('loadProject', dict(project=name))
    p.checkpoint = r.checkpoint
    return p

  def __init__(self):
    '''Creates a new blank project.'''
    r = _send('newProject')
    self.checkpoint = r.checkpoint

  def save(self, name):
    _send('saveProject', dict(checkpoint=self.checkpoint, project=name))

  def scalar(self, scalar):
    '''Fetches the value of a scalar. Returns either a double or a string.'''
    r = _send('getScalar', dict(checkpoint=self.checkpoint, scalar=scalar))
    if hasattr(r, 'double'):
      return r.double
    return r.string

  def sql(self, query, limit=None):
    r = _send('sql', dict(
      checkpoint=self.checkpoint,
      query=query,
      limit=limit or default_sql_limit,
      ), raw=True)
    return r['rows']

  def run_operation(self, operation, parameters):
    '''Runs an operation on the project with the given parameters.'''
    r = _send('runOperation',
        dict(checkpoint=self.checkpoint, operation=operation, parameters=parameters))
    self.checkpoint = r.checkpoint
    return self

  def __getattr__(self, attr):
    '''For any unknown names we return a function that tries to run an operation by that name.'''
    def f(**kwargs):
      params = {}
      for k, v in kwargs.items():
        params[k] = str(v)
      return self.run_operation(attr, params)
    return f


class LynxException(Exception):
  '''Raised when LynxKite indicates that an error has occured while processing a command.'''
  def __init__(self, error, command):
    super(LynxException, self).__init__(error)
    self.error = error
    self.command = command


def _request(endpoint, payload={}):
  '''Sends an HTTP request to LynxKite and returns the response when it arrives.'''
  data = json.dumps(payload).encode('utf-8')
  req = urllib.request.Request(
      os.environ.get('LYNXKITE_ADDRESS').rstrip('/') + '/' + endpoint.lstrip('/'),
      data=data,
      headers={'Content-Type': 'application/json'})
  with connection.open(req) as r:
    return r.read().decode('utf-8')


def _send(command, payload={}, raw=False):
  '''Sends a command to LynxKite and returns the response when it arrives.'''
  data = _request('/remote', dict(command=command, payload=payload))
  if raw:
    r = json.loads(data)
    if 'error' in r:
      raise LynxException(r['error'], r['request'])
  else:
    r = json.loads(data, object_hook=_asobject)
    if hasattr(r, 'error'):
      raise LynxException(r.error, r.request)
  return r


def _asobject(dic):
  '''Wraps the dict in a namespace for easier access. I.e. d["x"] becomes d.x.'''
  return types.SimpleNamespace(**dic)


reconnect()
