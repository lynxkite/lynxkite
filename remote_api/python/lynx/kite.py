'''Python interface for the LynxKite Remote API.

The default LynxKite connection parameters can be configured through the following environment
variables::

    LYNXKITE_ADDRESS=https://lynxkite.example.com/
    LYNXKITE_USERNAME=user@company
    LYNXKITE_PASSWORD=my_password
    LYNXKITE_PUBLIC_SSL_CERT=/tmp/lynxkite.crt

Example usage::

    import lynx.kite
    lk = lynx.kite.LynxKite()
    outputs = lk.run(json.loads(WORKSPACE_COPIED_FROM_UI))
    state = outputs['Create-example-graph_1', 'project'].stateId
    project = lk.get_project(state)
    scalars = {s.title: lk.get_scalar(s.id) for s in project.scalars}
    print(scalars['!vertex_count'].double)

The list of operations is not documented, but you can copy the invocation from a LynxKite project
history.
'''
import copy
import json
import os
import requests
import sys
import types

if sys.version_info.major < 3:
  raise Exception('At least Python version 3 is needed!')


class LynxKite:
  '''A connection to a LynxKite instance.

  Some LynxKite API methods take a connection argument which can be used to communicate with
  multiple LynxKite instances from the same session. If no arguments to the constructor are
  provided, then a connection is created using the following environment variables:
  ``LYNXKITE_ADDRESS``, ``LYNXKITE_USERNAME``, ``LYNXKITE_PASSWORD``,
  ``LYNXKITE_PUBLIC_SSL_CERT``, ``LYNXKITE_OAUTH_TOKEN``.
  '''

  def __init__(self, username=None, password=None, address=None, certfile=None, oauth_token=None):
    '''Creates a connection object.'''
    # Authentication and querying environment variables is deferred until the
    # first request.
    self._address = address
    self._username = username
    self._password = password
    self._certfile = certfile
    self._oauth_token = oauth_token
    self._session = None
    self._operation_names = None

  def operation_names(self):
    if not self._operation_names:
      self._operation_names = self._send('/remote/getOperationNames').names
    return self._operation_names

  def address(self):
    return self._address or os.environ['LYNXKITE_ADDRESS']

  def username(self):
    return self._username or os.environ.get('LYNXKITE_USERNAME')

  def password(self):
    return self._password or os.environ.get('LYNXKITE_PASSWORD')

  def certfile(self):
    return self._certfile or os.environ.get('LYNXKITE_PUBLIC_SSL_CERT')

  def oauth_token(self):
    return self._oauth_token or os.environ.get('LYNXKITE_OAUTH_TOKEN')

  def _login(self):
    if self.password():
      r = self._request(
          '/passwordLogin',
          dict(
              username=self.username(),
              password=self.password(),
              method='lynxkite'))
      r.raise_for_status()
    elif self.oauth_token():
      r = self._request(
          '/googleLogin',
          dict(id_token=self.oauth_token()))
      r.raise_for_status()
    else:
      raise Exception('No login credentials provided.')

  def _get_session(self):
    '''Create a new session or return the cached one. If the process was forked (if the pid
    has changed), then the cache is invalidated. See issue #5436.'''
    if self._session is None or self._pid != os.getpid():
      self._session = requests.Session()
      self._pid = os.getpid()
    return self._session

  def __del__(self):
    if self._session:
      self._session.close()

  def _method(self, method, endpoint, **kwargs):
    '''Sends an HTTP request to LynxKite and returns the response when it arrives.'''
    max_tries = 3
    for i in range(max_tries):
      r = getattr(self._get_session(), method)(
          self.address().rstrip('/') + '/' + endpoint.lstrip('/'),
          verify=self.certfile(),
          allow_redirects=False,
          **kwargs)
      if r.status_code < 400:
        return r
      if r.status_code == 401 and i + 1 < max_tries:  # Unauthorized.
        self._login()
        # And then retry via the "for" loop.
      elif r.status_code == 500:  # Internal server error.
        raise LynxException(r.text)
      else:
        r.raise_for_status()

  def _post(self, endpoint, **kwargs):
    return self._method('post', endpoint, **kwargs)

  def _get(self, endpoint, **kwargs):
    return self._method('get', endpoint, **kwargs)

  def _request(self, endpoint, payload={}):
    '''Sends an HTTP JSON request to LynxKite and returns the response when it arrives.'''
    data = json.dumps(payload)
    return self._post(endpoint, data=data, headers={'Content-Type': 'application/json'})

  def _send(self, command, payload={}, raw=False):
    '''Sends a command to LynxKite and returns the response when it arrives.'''
    data = self._request(command, payload).text
    if raw:
      r = json.loads(data)
    else:
      r = json.loads(data, object_hook=_asobject)
    return r

  def _ask(self, command, payload={}):
    '''Sends a JSON GET request.'''
    resp = self._get(
        command,
        params=dict(q=json.dumps(payload)),
        headers={'X-Requested-With': 'XMLHttpRequest'})
    return json.loads(resp.text, object_hook=_asobject)

  def get_directory_entry(self, path):
    '''Returns details about a LynxKite path. The returned object has the following fields:
    ``exists``, ``isWorkspace``, ``isSnapshot``, ``isDirectory``
    '''
    return self._send('/remote/getDirectoryEntry', dict(path=path))

  def get_prefixed_path(self, path):
    '''Resolves a path on a distributed file system. The path has to be specified using
    LynxKite's prefixed path syntax. (E.g. ``DATA$/my_file.csv``.)

    The returned object has an ``exists`` and a ``resolved`` attribute. ``resolved`` is a string
    containing the absolute path.
    '''
    return self._send('/remote/getPrefixedPath', dict(path=path))

  def get_parquet_metadata(self, path):
    '''Reads the metadata of a parquet file and returns the number of rows.'''
    r = self._send('/remote/getParquetMetadata', dict(path=path))
    return r

  def remove_name(self, name):
    '''Removes an object named ``name``.'''
    self._send('/remote/removeName', dict(name=name))

  def change_acl(self, file, readACL, writeACL):
    '''Sets the read and write access control list for a path in LynxKite.'''
    self._send('/remote/changeACL',
               dict(project=file, readACL=readACL, writeACL=writeACL))

  def list_dir(self, dir=''):
    '''List the objects in a directory.'''

    return self._send('/remote/list', dict(path=dir)).entries

  def upload(self, data, name=None):
    '''Uploads a file that can then be used in import methods.

      prefixed_path = lk.upload('id,name\\n1,Bob')
    '''
    if name is None:
      name = 'remote-api-upload'  # A hash will be added anyway.
    return self._post('/ajax/upload', files=dict(file=(name, data))).text

  def clean_file_system(self):
    """Deletes the data files which are not referenced anymore."""
    self._send('/remote/cleanFileSystem')

  def run(self, boxes, parameters=dict()):
    res = self._send(
        '/ajax/runWorkspace', dict(workspace=dict(boxes=boxes), parameters=parameters))
    return {(o.boxOutput.boxId, o.boxOutput.id): o for o in res.outputs}

  def get_scalar(self, guid):
    return self._ask('/ajax/scalarValue', dict(scalarId=guid))

  def get_project(self, state, path=''):
    return self._ask('/ajax/getProjectOutput', dict(id=state, path=path))

  def get_export_result(self, state):
    return self._ask('/ajax/getExportResultOutput', dict(stateId=state))

  def get_table(self, state, rows=-1):
    return self._ask('/ajax/getTableOutput', dict(id=state, sampleRows=rows))

  def import_box(self, boxes, box_id):
    '''Equivalent to clicking the import button for an import box. Returns the updated boxes.'''
    boxes = copy.deepcopy(boxes)
    for box in boxes:
      if box['id'] == box_id:
        import_result = self._send('/ajax/importBox', box)
        box['parameters']['imported_table'] = import_result.guid
        box['parameters']['last_settings'] = import_result.parameterSettings
        return boxes
    raise KeyError(box_id)

  def export_box(self, outputs, box_id):
    '''Equivalent to triggering the export. Returns the exportResult output.'''
    output = outputs[box_id, 'exported']
    assert output.kind == 'exportResult', 'Output is {}, not "exportResult"'.format(output.kind)
    assert output.success.enabled, 'Output has failed: {}'.format(output.success.disabledReason)
    export = self.get_export_result(output.stateId)
    if export.result.computeProgress != 1:
      scalar = self.get_scalar(export.result.id)
      assert scalar.string == 'Export done.', scalar.string
      export = self.get_export_result(output.stateId)
      assert export.result.computeProgress == 1, 'Failed to compute export result scalar.'
    return export

  def download_file(self, path):
    return self._get(
        'downloadFile',
        params=dict(q=json.dumps(dict(path=path, stripHeaders=False)))).content


class LynxException(Exception):
  '''Raised when LynxKite indicates that an error has occured while processing a command.'''

  def __init__(self, error):
    super(LynxException, self).__init__(error)
    self.error = error


def _asobject(dic):
  '''Wraps the dict in a namespace for easier access. I.e. d["x"] becomes d.x.'''
  return types.SimpleNamespace(**dic)


class PizzaKite(LynxKite):

  def __init__(self):
    super().__init__(address='https://pizzakite.lynxanalytics.com/')
    assert self.oauth_token(), 'Please set LYNXKITE_OAUTH_TOKEN.'
