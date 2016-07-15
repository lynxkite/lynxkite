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
    r = self.connection.send('sql', dict(
        checkpoint=self.checkpoint,
        query=query,
        limit=limit or default_sql_limit,
    ), raw=True)
    return r['rows']

  def globalSql(self, query, limit=None, **kwargs):
    r = self.connection.send('globalSql', dict(
      checkpoints=kwargs.items(),
      query=query,
      limit=limit or default_sql_limit,
      ), raw=True)
    return r['rows']

  def import_csv(self, files, table,
                 privacy=default_privacy,
                 columnNames=[],
                 delimiter=',',
                 mode='FAILFAST',
                 infer=True,
                 columnsToImport=[]):
    r = self.connection.send('importCSV',
                             dict(
                                 files=files,
                                 table=table,
                                 privacy=privacy,
                                 columnNames=columnNames,
                                 delimiter=delimiter,
                                 mode=mode,
                                 infer=infer,
                                 columnsToImport=columnsToImport))
    return r

  def import_hive(self, table, hiveTable,
                  privacy=default_privacy, columnsToImport=[]):
    r = self.connection.send('importHive',
                             dict(
                                 table=table,
                                 privacy=privacy,
                                 hiveTable=hiveTable,
                                 columnsToImport=columnsToImport))
    return r

  def import_jdbc(self, table, jdbcUrl, jdbcTable, keyColumn,
                  privacy=default_privacy, columnsToImport=[]):
    r = self.connection.send('importJdbc',
                             dict(
                                 table=table,
                                 jdbcUrl=jdbcUrl,
                                 privacy=privacy,
                                 jdbcTable=jdbcTable,
                                 keyColumn=keyColumn,
                                 columnsToImport=columnsToImport))
    return r

  def import_parquet(self, table, privacy=default_privacy, columnsToImport=[]):
    self._importFileWithSchema('Parquet', table, privacy, columnsToImport)

  def import_orc(self, table, privacy=default_privacy, columnsToImport=[]):
    self._importFileWithSchema('ORC', table, privacy, columnsToImport)

  def import_json(self, table, privacy=default_privacy, columnsToImport=[]):
    self._importFileWithSchema('Json', table, privacy, columnsToImport)

  def _importFileWithSchema(format, table, privacy, files, columnsToImport):
    r = self.connection.send('import' + format,
                             dict(
                                 table=table,
                                 privacy=privacy,
                                 files=files,
                                 columnsToImport=columnsToImport))
    return r

  def run_operation(self, operation, parameters):
    '''Runs an operation on the project with the given parameters.'''
    r = self.connection.send('runOperation',
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

  def __init__(self, error):
    super(LynxException, self).__init__(error)
    self.error = error


def _asobject(dic):
  '''Wraps the dict in a namespace for easier access. I.e. d["x"] becomes d.x.'''
  return types.SimpleNamespace(**dic)
