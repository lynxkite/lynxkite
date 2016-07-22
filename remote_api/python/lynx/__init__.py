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
import sys
import types
import urllib

if sys.version_info.major < 3:
  raise Exception('At least Python version 3 is needed!')

default_sql_limit = 1000
default_privacy = 'public-read'

_connection = None


def default_connection():
  global _connection
  if _connection is None:
    _connection = Connection(
        os.environ['LYNXKITE_ADDRESS'],
        os.environ.get('LYNXKITE_USERNAME'),
        os.environ.get('LYNXKITE_PASSWORD'))
  return _connection


def sql(query, limit=None, **kwargs):
  '''Runs global level SQL query with the syntax: lynx.sql("select * from `x|vertices`", x=p, limit=10),
  where p is an object that has a checkpoint, and giving the limit is optional'''
  checkpoints = {}
  for name, p in kwargs.items():
    checkpoints[name] = p.checkpoint
  r = _connection.send('globalSQL', dict(
      query=query,
      checkpoints=checkpoints
  ))
  return View(checkpoint=r['checkpoint'])

class View:
  def __init__(self, checkpoint):
    self.checkpoint = checkpoint

  def take(self, limit):
    r = _connection.send('exportViewToTableResult', dict(
      checkpoint=self.checkpoint,
      limit=limit
    ), raw=True)
    return r['rows']

  def df(self):
    """ Will return Pandas DataFrame when implemented. """
    return 0

  def exportJson(self, path):
    _connection.send('exportViewToJson', dict(
      checkpoint = self.checkpoint,
      path = path
    ))

def get_directory_entry(path, connection=None):
  connection = connection or default_connection()
  r = connection.send('getDirectoryEntry', dict(path=path))
  return r


def import_csv(
        files,
        table,
        privacy=default_privacy,
        columnNames=[],
        delimiter=',',
        mode='FAILFAST',
        infer=True,
        columnsToImport=[],
        view=False,
        connection=None):
  return _import_or_create_view(
      "CSV",
      view,
      dict(table=table,
           files=files,
           privacy=privacy,
           columnNames=columnNames,
           delimiter=delimiter,
           mode=mode,
           infer=infer,
           columnsToImport=columnsToImport),
      connection)


def import_hive(
        table,
        hiveTable,
        privacy=default_privacy,
        columnsToImport=[],
        connection=None):
  return _import_or_create_view(
      "Hive",
      view,
      dict(
          table=table,
          privacy=privacy,
          hiveTable=hiveTable,
          columnsToImport=columnsToImport),
      connection)


def import_jdbc(
        table,
        jdbcUrl,
        jdbcTable,
        keyColumn,
        privacy=default_privacy,
        columnsToImport=[],
        view=False,
        connection=None):
  return _import_or_create_view(
      "Jdbc",
      view,
      dict(table=table,
           jdbcUrl=jdbcUrl,
           privacy=privacy,
           jdbcTable=jdbcTable,
           keyColumn=keyColumn,
           columnsToImport=columnsToImport),
      connection)


def import_parquet(
        table,
        privacy=default_privacy,
        columnsToImport=[],
        view=False,
        connection=None):
  return _import_or_create_view(
      "Parquet",
      view,
      dict(table=table,
           privacy=privacy,
           columnsToImport=columnsToImport),
      connection)


def import_orc(
        table,
        privacy=default_privacy,
        columnsToImport=[],
        view=False,
        connection=None):
  return _import_or_create_view(
      "ORC",
      view,
      dict(table=table,
           privacy=privacy,
           columnsToImport=columnsToImport),
      connection)


def import_json(
        table,
        privacy=default_privacy,
        columnsToImport=[],
        view=False,
        connection=None):
  return _import_or_create_view(
      "Json",
      view,
      dict(table=table,
           privacy=privacy,
           columnsToImport=columnsToImport),
      connection)


def _import_or_create_view(format, view, dict, connection):
  connection = connection or default_connection()
  endpoint = ("createView" if view else "import") + format
  return connection.send(endpoint, dict)


class Connection(object):

  '''A connection to a LynxKite instance.

  Some LynxKite API methods take a connection argument which can be used to communicate with
  multiple LynxKite instances from the same session. If this argument is not provided, the default
  connection is used instead. The default connection is configured via the LYNXKITE_ADDRESS,
  LYNXKITE_USERNAME, and LYNXKITE_PASSWORD environment variables.
  '''

  def __init__(self, address, username=None, password=None):
    '''Creates a connection object, performing authentication if necessary.'''
    self.address = address
    self.username = username
    self.password = password
    cj = http.cookiejar.CookieJar()
    self.opener = urllib.request.build_opener(
        urllib.request.HTTPCookieProcessor(cj))
    if username:
      self.login()

  def login(self):
    self.request(
        '/passwordLogin',
        dict(
            username=self.username,
            password=self.password))

  def request(self, endpoint, payload={}):
    '''Sends an HTTP request to LynxKite and returns the response when it arrives.'''
    data = json.dumps(payload).encode('utf-8')
    req = urllib.request.Request(
        self.address.rstrip('/') + '/' + endpoint.lstrip('/'),
        data=data,
        headers={'Content-Type': 'application/json'})
    max_tries = 3
    for i in range(max_tries):
      try:
        with self.opener.open(req) as r:
          return r.read().decode('utf-8')
      except urllib.error.HTTPError as err:
        if err.code == 401:  # Unauthorized.
          self.login()
          # And then retry via the "for" loop.
        if err.code == 500:  # Unauthorized.
          raise LynxException(err.read())
        else:
          raise err

  def send(self, command, payload={}, raw=False):
    '''Sends a command to LynxKite and returns the response when it arrives.'''
    data = self.request('/remote/' + command, payload)
    if raw:
      r = json.loads(data)
    else:
      r = json.loads(data, object_hook=_asobject)
    return r


class Project(object):

  '''Represents an unanchored LynxKite project.

  This project is not automatically saved to the LynxKite project directories.
  '''
  @staticmethod
  def load(name, connection=None):
    '''Loads an existing LynxKite project.'''
    connection = connection or default_connection()
    p = Project(connection)
    r = connection.send('loadProject', dict(project=name))
    p.checkpoint = r.checkpoint
    return p

  def __init__(self, connection=None):
    '''Creates a new blank project.'''
    self.connection = connection or default_connection()
    r = self.connection.send('newProject')
    self.checkpoint = r.checkpoint

  def save(self, name):
    self.connection.send(
        'saveProject',
        dict(
            checkpoint=self.checkpoint,
            project=name))

  def scalar(self, scalar):
    '''Fetches the value of a scalar. Returns either a double or a string.'''
    r = self.connection.send(
        'getScalar',
        dict(
            checkpoint=self.checkpoint,
            scalar=scalar))
    if hasattr(r, 'double'):
      return r.double
    return r.string

  def sql(self, query, limit=None):
    r = self.connection.send('projectSQL', dict(
        checkpoint=self.checkpoint,
        query=query,
        limit=limit or default_sql_limit,
    ), raw=True)
    return r['rows']

  def run_operation(self, operation, parameters):
    '''Runs an operation on the project with the given parameters.'''
    r = self.connection.send('runOperation',
                             dict(checkpoint=self.checkpoint, operation=operation, parameters=parameters))
    self.checkpoint = r.checkpoint
    return self

  def compute(self):
    return self.connection.send(
        'computeProject', dict(checkpoint=self.checkpoint))

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

  def __init__(self, error):
    super(LynxException, self).__init__(error)
    self.error = error


def _asobject(dic):
  '''Wraps the dict in a namespace for easier access. I.e. d["x"] becomes d.x.'''
  return types.SimpleNamespace(**dic)
