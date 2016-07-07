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


default_sql_limit = 1000
default_privacy = "public-read"

def _init():
  '''Runs when the module is loaded. Reads configuration and creates response pipe.'''
  global server_pipe, response_pipe_int, response_pipe_ext
  server_pipe = _fromkiterc('KITE_API_PIPE')
  # This INTERNAL/EXTERNAL configuration is used when running in a Docker container.
  # INTERNAL is the path where we see the file. EXTERNAL is the same from LynxKite's perspective.
  response_dir_int = os.environ.get('KITE_API_PIPE_RESPONSE_INTERNAL') or tempfile.gettempdir()
  response_dir_ext = os.environ.get('KITE_API_PIPE_RESPONSE_EXTERNAL') or response_dir_int
  response_pipe_int = '{}/pipe-{}'.format(response_dir_int, os.getpid())
  response_pipe_ext = '{}/pipe-{}'.format(response_dir_ext, os.getpid())
  os.mkfifo(response_pipe_int)
  atexit.register(_cleanup)


def _cleanup():
  '''Runs at exit to remove the response pipe.'''
  os.remove(response_pipe_int)


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
  def __init__(self, error, command):
    super(LynxException, self).__init__(error)
    self.error = error
    self.command = command


def _send(command, payload={}, raw=False):
  '''Sends a command to LynxKite and returns the response when it arrives.'''
  msg = json.dumps(dict(command=command, payload=payload, responsePipe=response_pipe_ext))
  with open(server_pipe, 'w') as p:
    p.write(msg)
  with open(response_pipe_int) as p:
    data = p.read()
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


def _fromkiterc(variable):
  '''Returns the value of a variable defined in .kiterc.'''
  load_kiterc = '''
      KITE_SITE_CONFIG=${KITE_SITE_CONFIG:-$HOME/.kiterc}
      if [ -f "$KITE_SITE_CONFIG" ]; then
        source "$KITE_SITE_CONFIG"
      fi
      if [ -f "$KITE_SITE_CONFIG_OVERRIDES" ]; then
        source "$KITE_SITE_CONFIG_OVERRIDES"
      fi
      '''
  # We will source kiterc, then print the variable we want.
  command = '{}\n echo ${}'.format(load_kiterc, variable)
  return subprocess.check_output(command, shell=True, executable='/bin/bash').strip()


_init()
