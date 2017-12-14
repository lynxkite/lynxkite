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
'''
import copy
import json
import os
import queue
import random
import requests
import sys
import types
import calendar
from croniter import croniter
import datetime

if sys.version_info.major < 3:
  raise Exception('At least Python version 3 is needed!')


class TableSnapshotSequence:
  '''A snapshot sequence representing a list of tables in LynxKite.
  '''

  def __init__(self, location, cron_str, from_date, to_date):
    self._location = location
    self._cron_str = cron_str
    self._from_date = from_date
    self._to_date = to_date

  def snapshots(self, lk):
    # We want to include the from_date if it matches the cron format.
    i = croniter(self._cron_str, self._from_date - datetime.timedelta(seconds=1))
    t = []
    while True:
      dt = i.get_next(datetime.datetime)
      if dt > self._to_date:
        break
      name = str(dt)
      t.append(self._location + '/' + name)
    return t

  def table(self, lk):
    paths = self.snapshots(lk)
    chain = None
    for path in paths:
      snapshot = lk.importSnapshot(path=path)
      if chain:
        chain = lk.sql2(
            chain, snapshot, sql='select * from one union all select * from two')
      else:
        chain = snapshot.sql1(sql='select * from input')
    return chain


def _python_name(name):
  '''Transforms a space separated string into a camelCase format.

  The operation "Use base project as segmentation" will be called as
  ``useBaseProjectAsSegmentation``. Dashes are ommitted.
  '''
  name = ''.join([c if c.isalnum() or c == ' ' else '' for c in name])
  return ''.join(
      [x.lower() for x in name.split()][:1] +
      [x.lower().capitalize() for x in name.split()][1:])


_anchor_box = {
    'id': 'anchor',
    'operationId': 'Anchor',
    'parameters': {},
    'x': 0, 'y': 0,
    'inputs': {},
    'parametricParameters': {}
}


class State:
  '''Represents a named output plug of a box.

  It can recursively store the boxes which are connected to the input plugs of
  the box of this state.
  '''

  def __init__(self, box, output_plug_name):
    self.output_plug_name = output_plug_name
    self.box = box

  def __getattr__(self, name):

    def f(**kwargs):
      inputs = self.box.bc.inputs(name)
      # This chaining syntax only allowed for boxes with exactly one input.
      assert len(inputs) > 0, '{} does not have an input'.format(name)
      assert len(inputs) < 2, '{} has more than one input'.format(name)
      [input_name] = inputs
      return new_box(
          self.box.bc, name, inputs={input_name: self}, parameters=kwargs)

    if not name in self.box.bc.box_names():
      raise AttributeError('{} is not defined on {}'.format(name, self))
    return f

  def __dir__(self):
    return super().__dir__() + self.box.bc.box_names()

  def __str__(self):
    return "Output {} of box {}".format(self.output_plug_name, self.box)


def new_box(bc, operation, inputs, parameters):
  if isinstance(operation, str):
    outputs = bc.outputs(operation)
  else:
    outputs = operation.outputs()
  if len(outputs) == 1:
    # Special case: single output boxes function as state as well.
    return SingleOutputBox(bc, operation, outputs[0], inputs, parameters)
  else:
    return Box(bc, operation, inputs, parameters)


class Box:
  '''Represents a box in a workspace segment.

  It can store workspace segments, connected to its input plugs.
  '''

  def __init__(self, box_catalog, operation, inputs, parameters):
    self.bc = box_catalog
    self.operation = operation
    if isinstance(operation, str):
      exp_inputs = set(self.bc.inputs(operation))
      self.outputs = set(self.bc.outputs(operation))
    else:
      assert isinstance(operation, Workspace), "{} is not string or workspace".format(operation)
      exp_inputs = set(operation.inputs())
      self.outputs = set(operation.outputs())
    got_inputs = inputs.keys()
    assert got_inputs == exp_inputs, 'Got box inputs: {}. Expected: {}'.format(
        got_inputs, exp_inputs)
    self.inputs = inputs
    self.parameters = parameters
    self.parametric_parameters = {}  # TODO: implement it (separate simple and parametric)

  def to_json(self, id_resolver, workspace_root):
    '''Creates the json representation of a box in a workspace.

    The inputs have to be connected, and all the attributes have to be
    defined when we call this.
    '''
    def input_state(state):
      return {'boxId': id_resolver(state.box), 'id': state.output_plug_name}

    if isinstance(self.operation, str):
      operationId = self.bc.operation_id(self.operation)
    else:
      operationId = workspace_root + self.operation.name()
    return {
        'id': id_resolver(self),
        'operationId': operationId,
        'parameters': self.parameters,
        'x': 0, 'y': 0,
        'inputs': {plug: input_state(state) for plug, state in self.inputs.items()},
        'parametricParameters': self.parametric_parameters}

  def __getitem__(self, index):
    if index not in self.outputs:
      raise KeyError(index)
    return State(self, index)

  def __str__(self):
    return "Operation {} with parameters {} and inputs {}".format(
        self.operationId,
        self.parameters,
        self.inputs)


class SingleOutputBox(Box, State):

  def __init__(self, box_catalog, operation, output_name, inputs, parameters):
    Box.__init__(self, box_catalog, operation, inputs, parameters)
    State.__init__(self, self, output_name)


class BoxCatalog:
  '''Stores box metadata.

  Offers utility functions to query box metadata information.
  '''

  def __init__(self, boxes):
    self.bc = boxes  # Dictionary, the keys are the Python names of the boxes.

  def inputs(self, name):
    return self.bc[name].inputs

  def outputs(self, name):
    return self.bc[name].outputs

  def operation_id(self, name):
    return self.bc[name].operationId

  def box_names(self):
    return list(self.bc.keys())


class Workspace:
  '''Immutable class representing a LynxKite workspace'''

  def __init__(self, name, terminal_boxes, input_boxes=[]):
    self._name = name
    self._all_boxes = set()
    self._box_ids = dict()
    self._next_id = 0
    self._inputs = [inp.parameters['name'] for inp in input_boxes]
    self._outputs = [
        outp.parameters['name'] for outp in terminal_boxes
        if outp.operation == 'output']
    self._bc = terminal_boxes[0].bc

    # We enumerate and add all upstream boxes for terminal_boxes via a simple
    # BFS.
    to_process = queue.Queue()
    for box in terminal_boxes:
      to_process.put(box)
      self._add_box(box)
    while not to_process.empty():
      box = to_process.get()
      for input_state in box.inputs.values():
        parent_box = input_state.box
        if parent_box not in self._all_boxes:
          self._add_box(parent_box)
          to_process.put(parent_box)

  def _add_box(self, box):
    self._all_boxes.add(box)
    self._box_ids[box] = "box_{}".format(self._next_id)
    self._next_id += 1

  def id_of(self, box):
    return self._box_ids[box]

  def to_json(self, workspace_root):
    normal_boxes = [
        box.to_json(self.id_of, workspace_root) for box in self._all_boxes]
    return [_anchor_box] + normal_boxes

  def required_workspaces(self):
    return [
        box.operation for box in self._all_boxes
        if isinstance(box.operation, Workspace)]

  def inputs(self):
    return list(self._inputs)

  def outputs(self):
    return list(self._outputs)

  def name(self):
    return self._name

  def __call__(self, *args, **kwargs):
    inputs = dict(zip(self.inputs(), args))
    return new_box(self._bc, self, inputs=inputs, parameters=kwargs)


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
    self._box_catalog = None

  def operation_names(self):
    if not self._operation_names:
      self._operation_names = self.box_catalog().box_names()
    return self._operation_names

  def box_catalog(self):
    if not self._box_catalog:
      bc = self._ask('/ajax/boxCatalog').boxes
      boxes = {}
      for box in bc:
        if box.categoryId != 'Custom boxes':
          boxes[_python_name(box.operationId)] = box
      self._box_catalog = BoxCatalog(boxes)
    return self._box_catalog

  def __dir__(self):
    return super().__dir__() + self.operation_names()

  def __getattr__(self, name):

    def f(*args, **kwargs):
      inputs = dict(zip(self.box_catalog().inputs(name), args))
      return new_box(self.box_catalog(), name, inputs=inputs, parameters=kwargs)

    if not name in self.operation_names():
      raise AttributeError('{} is not defined'.format(name))
    return f

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

  def remove_name(self, name, force=False):
    '''Removes an object named ``name``.'''
    self._send('/remote/removeName', dict(name=name, force=force))

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

  def run_workspace(self, ws, save_under_root=None):
    ws_root = save_under_root
    if ws_root is None:
      ws_root = 'tmp_{}/'.format(''.join(random.choice('0123456789ABCDEF') for i in range(16)))
    needed_ws = set()
    ws_queue = queue.Queue()
    ws_queue.put(ws)
    while not ws_queue.empty():
      nws = ws_queue.get()
      for rws in nws.required_workspaces():
        if rws not in needed_ws:
          needed_ws.add(rws)
          ws_queue.put(rws)
    for rws in needed_ws:
      self.save_workspace(ws_root + rws.name(), rws.to_json(ws_root))
    return self.run(ws.to_json(ws_root))
    # TODO: clean up saved workspaces if save_under_root is not set. And
    # also save main workspace if it is set.

  def get_state_id(self, state):
    ws = Workspace('Anonymous', [state.box])
    workspace_outputs = self.run_workspace(ws)
    return workspace_outputs[
        ws.id_of(state.box), state.output_plug_name].stateId

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
        import_result = self._send('/ajax/importBox', {'box': box})
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

  def save_workspace(self, path, boxes, overwrite=True):
    if not overwrite or not self.get_directory_entry(path).exists:
      self._send('/ajax/createWorkspace', dict(name=path))
    return self._send(
        '/ajax/setWorkspace',
        dict(reference=dict(top=path, customBoxStack=[]), workspace=dict(boxes=boxes)))

  def save_snapshot(self, path, stateId):
    return self._send(
        '/ajax/createSnapshot',
        dict(name=path, id=stateId))

  def create_dir(self, path, privacy='public-read'):
    return self._send(
        '/ajax/createDirectory',
        dict(name=path, privacy=privacy))


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
