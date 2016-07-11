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
default_privacy = "public-read"

def connect():
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

  def import_csv(self, files, table,
                privacy = default_privacy,
                columnNames = [],
                delimiter = ",",
                mode = "FAILFAST",
                infer = True,
                columnsToImport = []):
    r = _send("importCSV",
              dict(
                files = files,
                table = table,
                privacy = privacy,
                columnNames = columnNames,
                delimiter = delimiter,
                mode = mode,
                infer = infer,
                columnsToImport = columnsToImport))
    return r

  def import_hive(self, table, hiveTable, privacy = default_privacy, columnsToImport = []):
    r = _send("importHive",
              dict(
                table = table,
                privacy = privacy,
                hiveTable = hiveTable,
                columnsToImport = columnsToImport))
    return r

  def import_jdbc(self, table, jdbcUrl, jdbcTable, keyColumn,
                  privacy = default_privacy, columnsToImport = []):
    r = _send("importJdbc",
              dict(
                table = table,
                jdbcUrl = jdbcUrl,
                privacy = privacy,
                jdbcTable = jdbcTable,
                keyColumn = keyColumn,
                columnsToImport = columnsToImport))
    return r

  def import_parquet(self, table, privacy = default_privacy, columnsToImport = []):
    self._importFileWithSchema("Parquet", table, privacy, columnsToImport)

  def import_orc(self, table, privacy = default_privacy, columnsToImport = []):
    self._importFileWithSchema("ORC", table, privacy, columnsToImport)

  def import_json(self, table, privacy = default_privacy, columnsToImport = []):
    self._importFileWithSchema("Json", table, privacy, columnsToImport)

  def _importFileWithSchema(format, table, privacy, files, columnsToImport):
    r = _send("import" + format,
              dict(
                table = table,
                privacy = privacy,
                files = files,
                columnsToImport = columnsToImport))
    return r


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
  def __init__(self, error):
    super(LynxException, self).__init__(error)
    self.error = error


def _request(endpoint, payload={}):
  '''Sends an HTTP request to LynxKite and returns the response when it arrives.'''
  data = json.dumps(payload).encode('utf-8')
  req = urllib.request.Request(
      os.environ.get('LYNXKITE_ADDRESS').rstrip('/') + '/' + endpoint.lstrip('/'),
      data=data,
      headers={'Content-Type': 'application/json'})
  max_tries = 3
  for i in range(max_tries):
    try:
      with connection.open(req) as r:
        return r.read().decode('utf-8')
    except urllib.error.HTTPError as err:
      if err.code == 401: # Unauthorized.
        connect()
        # And then retry via the "for" loop.
      if err.code == 500: # Unauthorized.
        raise LynxException(err.read())
      else:
        raise err


def _send(command, payload={}, raw=False):
  '''Sends a command to LynxKite and returns the response when it arrives.'''
  data = _request('/remote/' + command, payload)
  if raw:
    r = json.loads(data)
  else:
    r = json.loads(data, object_hook=_asobject)
  return r


def _asobject(dic):
  '''Wraps the dict in a namespace for easier access. I.e. d["x"] becomes d.x.'''
  return types.SimpleNamespace(**dic)


connect()
