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
    outputs = lk.fetch_states(json.loads(WORKSPACE_COPIED_FROM_UI))
    state = outputs['Create-example-graph_1', 'project'].stateId
    project = lk.get_project(state)
    scalars = {s.title: lk.get_scalar(s.id) for s in project.scalars}
    print(scalars['!vertex_count'].double)
'''
import copy
import json
import os
import random
import sys
import types
import datetime
import inspect
import re
import itertools
import collections
from typing import (Dict, List, Union, Callable, Any, Tuple, Iterable, Set, NewType, Iterator,
                    TypeVar, cast)

import requests
from croniter import croniter


if sys.version_info.major < 3:
  raise Exception('At least Python version 3 is needed!')


def _timestamp_is_valid(dt: datetime.datetime, cron_str: str) -> bool:
  '''Checks whether ``dt`` is valid according to cron_str.'''
  i = croniter(cron_str, dt - datetime.timedelta(seconds=1))
  return i.get_next(datetime.datetime) == dt


def _step_back(cron_str: str, date: datetime.datetime, delta: int) -> datetime.datetime:
  cron = croniter(cron_str, date)
  start_date = date
  for _ in range(delta):
    start_date = cron.get_prev(datetime.datetime)
  return start_date


def _random_ws_folder() -> str:
  return 'tmp_workspaces/{}'.format(''.join(random.choices('0123456789ABCDEF', k=16)))


def _normalize_path(path: str) -> str:
  '''Removes leading, trailing slashes and slash-duplicates.'''
  return re.sub('/+', '/', path).strip('/')


def escape(s: str) -> str:
  '''Sanitizes a string for injecting into a generated SQL query.'''
  return s.replace('\\', '\\\\').replace('"', '\\"').replace("'", "\\'")


class ParametricParameter:
  '''Represents a parametric parameter value. The short alias `pp` is defined for this type.'''

  def __init__(self, parametric_expr: str) -> None:
    self._value = parametric_expr

  def __str__(self) -> str:
    return self._value


pp = ParametricParameter


class WorkspaceParameter:
  ''' Represents a workspace parameter declaration.'''

  def __init__(self, name: str, kind: str, default_value: str = '') -> None:
    self.name = name
    self.kind = kind
    self.default_value = default_value

  def to_json(self) -> Dict[str, str]:
    return dict(id=self.name, kind=self.kind, defaultValue=self.default_value)


def text(name: str, default: str = '') -> WorkspaceParameter:
  '''Helper function to make it easy to define a text kind ws parameter.'''
  return WorkspaceParameter(name, 'text', default_value=default)


class BoxCatalog:
  '''Stores box metadata.

  Offers utility functions to query box metadata information.
  '''

  def __init__(self, boxes: Dict[str, types.SimpleNamespace]) -> None:
    self.bc = boxes  # Dictionary, the keys are the Python names of the boxes.

  def inputs(self, name: str) -> List[str]:
    return self.bc[name].inputs

  def outputs(self, name: str) -> List[str]:
    return self.bc[name].outputs

  def operation_id(self, name: str) -> str:
    return self.bc[name].operationId

  def box_names(self) -> List[str]:
    return list(self.bc.keys())


SerializedBox = NewType('SerializedBox', Dict[str, Any])


class LynxKite:
  '''A connection to a LynxKite instance.

  Some LynxKite API methods take a connection argument which can be used to communicate with
  multiple LynxKite instances from the same session. If no arguments to the constructor are
  provided, then a connection is created using the following environment variables:
  ``LYNXKITE_ADDRESS``, ``LYNXKITE_USERNAME``, ``LYNXKITE_PASSWORD``,
  ``LYNXKITE_PUBLIC_SSL_CERT``, ``LYNXKITE_OAUTH_TOKEN``.
  '''

  def __init__(self, username: str = None, password: str = None, address: str = None,
               certfile: str = None, oauth_token: str = None,
               box_catalog: BoxCatalog = None) -> None:
    '''Creates a connection object.'''
    # Authentication and querying environment variables is deferred until the
    # first request.
    self._address = address
    self._username = username
    self._password = password
    self._certfile = certfile
    self._oauth_token = oauth_token
    self._session = None
    self._pid = None
    self._operation_names: List[str] = None
    self._box_catalog = box_catalog  # TODO: create standard offline box catalog

  def operation_names(self) -> List[str]:
    if not self._operation_names:
      self._operation_names = self.box_catalog().box_names()
    return self._operation_names

  def box_catalog(self) -> BoxCatalog:
    if not self._box_catalog:
      bc = self._ask('/ajax/boxCatalog').boxes
      boxes = {}
      for box in bc:
        if box.categoryId != 'Custom boxes':
          boxes[_python_name(box.operationId)] = box
      self._box_catalog = BoxCatalog(boxes)
    return self._box_catalog

  def __dir__(self) -> Iterable[str]:
    return itertools.chain(super().__dir__(), self.operation_names())

  def __getattr__(self, name) -> Callable:

    def f(*args, **kwargs):
      inputs = dict(zip(self.box_catalog().inputs(name), args))
      box = _new_box(self.box_catalog(), self, name, inputs=inputs, parameters=kwargs)
      # If it is an import box, we trigger the import here.
      import_box_names = ['importCSV', 'importJSON', 'importFromHive',
                          'importParquet', 'importORC', 'importJDBC']
      if name in import_box_names:
        box_json = box.to_json(id_resolver=lambda _: 'untriggered_import_box', workspace_root='')
        import_result = self._send('/ajax/importBox', {'box': box_json})
        box.parameters['imported_table'] = import_result.guid
        box.parameters['last_settings'] = import_result.parameterSettings
      return box

    if not name in self.operation_names():
      raise AttributeError('{} is not defined'.format(name))
    return f

  def sql(self, sql: str, *args, **kwargs) -> 'Box':
    '''Shorthand for sql1, sql2, ..., sql10 boxes'''
    num_inputs = len(args)
    assert num_inputs > 0, 'SQL needs at least one input.'
    assert num_inputs < 11, 'SQL can have at most ten inputs.'
    name = 'sql{}'.format(num_inputs)
    inputs = dict(zip(self.box_catalog().inputs(name), args))
    kwargs['sql'] = sql
    return _new_box(self.box_catalog(), self, name, inputs=inputs, parameters=kwargs)

  def address(self) -> str:
    return self._address or os.environ['LYNXKITE_ADDRESS']

  def username(self) -> str:
    return self._username or os.environ.get('LYNXKITE_USERNAME')

  def password(self) -> str:
    return self._password or os.environ.get('LYNXKITE_PASSWORD')

  def certfile(self) -> str:
    return self._certfile or os.environ.get('LYNXKITE_PUBLIC_SSL_CERT')

  def oauth_token(self) -> str:
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

  def __del__(self) -> None:
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

  def get_directory_entry(self, path: str):
    '''Returns details about a LynxKite path. The returned object has the following fields:
    ``exists``, ``isWorkspace``, ``isSnapshot``, ``isDirectory``
    '''
    return self._send('/remote/getDirectoryEntry', dict(path=path))

  def get_prefixed_path(self, path: str):
    '''Resolves a path on a distributed file system. The path has to be specified using
    LynxKite's prefixed path syntax. (E.g. ``DATA$/my_file.csv``.)

    The returned object has an ``exists`` and a ``resolved`` attribute. ``resolved`` is a string
    containing the absolute path.
    '''
    return self._send('/remote/getPrefixedPath', dict(path=path))

  def get_parquet_metadata(self, path: str):
    '''Reads the metadata of a parquet file and returns the number of rows.'''
    r = self._send('/remote/getParquetMetadata', dict(path=path))
    return r

  def remove_name(self, name: str, force: bool = False):
    '''Removes an object named ``name``.'''
    self._send('/remote/removeName', dict(name=name, force=force))

  def change_acl(self, file: str, readACL: str, writeACL: str):
    '''Sets the read and write access control list for a path in LynxKite.'''
    self._send('/remote/changeACL',
               dict(project=file, readACL=readACL, writeACL=writeACL))

  def list_dir(self, dir: str = '') -> List[str]:
    '''List the objects in a directory.'''

    return self._send('/remote/list', dict(path=dir)).entries

  def upload(self, data: bytes, name: str = None):
    '''Uploads a file that can then be used in import methods.

      prefixed_path = lk.upload('id,name\\n1,Bob')
    '''
    if name is None:
      name = 'remote-api-upload'  # A hash will be added anyway.
    return self._post('/ajax/upload', files=dict(file=(name, data))).text

  def clean_file_system(self) -> None:
    """Deletes the data files which are not referenced anymore."""
    self._send('/remote/cleanFileSystem')

  def fetch_states(self, boxes: List[SerializedBox],
                   parameters: Dict = dict()) -> Dict[Tuple[str, str], types.SimpleNamespace]:
    res = self._send(
        '/ajax/runWorkspace', dict(workspace=dict(boxes=boxes), parameters=parameters))
    return {(o.boxOutput.boxId, o.boxOutput.id): o for o in res.outputs}

  def save_workspace_recursively(self, ws: 'Workspace',
                                 save_under_root: str = None) -> Tuple[str, str]:
    if save_under_root is None:
      ws_root = _random_ws_folder()
    else:
      ws_root = save_under_root
    needed_ws: Set[Workspace] = set()
    ws_queue = collections.deque([ws])
    while len(ws_queue):
      nws = ws_queue.pop()
      for rws in nws.required_workspaces():
        if rws not in needed_ws:
          needed_ws.add(rws)
          ws_queue.append(rws)
    # Check name duplication in required workspaces
    names = list(rws.name() for rws in needed_ws)
    if len(needed_ws) != len(set(rws.name() for rws in needed_ws)):
      duplicates = [k for k, v in collections.Counter(names).items() if v > 1]
      raise Exception(f'Duplicate custom box name(s): {duplicates}')
    for rws in needed_ws:
      self.save_workspace(ws_root + '/' + rws.name(), _layout(rws.to_json(ws_root)))
    if save_under_root is not None:
      # Check if the "main" ws name conflicts with one of the custom box names
      if ws.name() in names:
        raise Exception(f'Duplicate name: {ws.name()}')
      self.save_workspace(
          save_under_root + '/' + ws.name(), _layout(ws.to_json(save_under_root)))
    # If saved, we return the full name of the main workspace also.
    return (ws_root,
            _normalize_path(save_under_root + '/' + ws.name())
            if (save_under_root is not None) else '')

  def fetch_workspace_output_states(self, ws: 'Workspace',
                                    save_under_root: str = None,
                                    ) -> Dict[Tuple[str, str], types.SimpleNamespace]:
    ws_root, _ = self.save_workspace_recursively(ws, save_under_root)
    return self.fetch_states(ws.to_json(ws_root))
    # TODO: clean up saved workspaces if save_under_root is not set.

  def get_state_id(self, state: 'State') -> str:
    ws = Workspace('Anonymous', [state.box])
    workspace_outputs = self.fetch_workspace_output_states(ws)
    box_id = ws.id_of(state.box)
    plug = state.output_plug_name
    output = workspace_outputs[box_id, plug]
    assert output.success.enabled, 'Output `{}` of `{}` has failed: {}'.format(
        plug, box_id, output.success.disabledReason)
    return output.stateId

  def get_scalar(self, guid: str) -> types.SimpleNamespace:
    return self._ask('/ajax/scalarValue', dict(scalarId=guid))

  def get_project(self, state: str, path: str = '') -> types.SimpleNamespace:
    return self._ask('/ajax/getProjectOutput', dict(id=state, path=path))

  def get_export_result(self, state: str) -> types.SimpleNamespace:
    return self._ask('/ajax/getExportResultOutput', dict(stateId=state))

  def get_table_data(self, state: str, limit: int = -1) -> types.SimpleNamespace:
    return self._ask('/ajax/getTableOutput', dict(id=state, sampleRows=limit))

  def get_workspace(self, path: str) -> List[types.SimpleNamespace]:
    response = self._ask('/ajax/getWorkspace', dict(top=path, customBoxStack=[]))
    return response.workspace.boxes

  # TODO: deprecate?
  def import_box(self, boxes: List[SerializedBox], box_id: str) -> List[SerializedBox]:
    '''Equivalent to clicking the import button for an import box. Returns the updated boxes.'''
    boxes = copy.deepcopy(boxes)
    for box in boxes:
      if box['id'] == box_id:
        import_result = self._send('/ajax/importBox', {'box': box})
        box['parameters']['imported_table'] = import_result.guid
        box['parameters']['last_settings'] = import_result.parameterSettings
        return boxes
    raise KeyError(box_id)

  # TODO: deprecate?
  def export_box(self, outputs: Dict[Tuple[str, str], types.SimpleNamespace],
                 box_id: str) -> types.SimpleNamespace:
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

  def download_file(self, path: str) -> bytes:
    return self._get(
        'downloadFile',
        params=dict(q=json.dumps(dict(path=path, stripHeaders=False)))).content

  def save_workspace(self, path: str, boxes: List[SerializedBox], overwrite: bool = True):
    path = _normalize_path(path)
    if not overwrite or not self.get_directory_entry(path).exists:
      self._send('/ajax/createWorkspace', dict(name=path))
    return self._send(
        '/ajax/setWorkspace',
        dict(reference=dict(top=path, customBoxStack=[]), workspace=dict(boxes=boxes)))

  def save_snapshot(self, path: str, stateId: str):
    return self._send(
        '/ajax/createSnapshot',
        dict(name=path, id=stateId))

  def create_dir(self, path: str, privacy: str = 'public-read'):
    return self._send(
        '/ajax/createDirectory',
        dict(name=path, privacy=privacy))

  def _workspace(self,
                 name: str = None,
                 parameters: List[WorkspaceParameter] = [],
                 with_side_effects: bool = False
                 ) -> Callable[[Callable[..., Dict[str, 'State']]], 'Workspace']:
    se_collector = SideEffectCollector()

    def ws_decorator(builder_fn):
      real_name = builder_fn.__name__ if not name else name
      if with_side_effects:
        names = list(inspect.signature(builder_fn).parameters.keys())[1:]
      else:
        names = inspect.signature(builder_fn).parameters.keys()
      inputs = [self.input(name=name) for name in names]
      if with_side_effects:
        results = builder_fn(se_collector, *inputs)
      else:
        results = builder_fn(*inputs)
      if results:
        outputs = [state.output(name=name) for name, state in results.items()]
      else:
        outputs = []
      return Workspace(name=real_name,
                       terminal_boxes=outputs + se_collector.direct_children,
                       trigger_paths=list(se_collector.all_triggerables()),
                       input_boxes=inputs,
                       ws_parameters=parameters)
    return ws_decorator

  def workspace(self,
                name: str = None,
                parameters: List[WorkspaceParameter] = []
                ) -> Callable[[Callable[..., Dict[str, 'State']]], 'Workspace']:
    return self._workspace(name, parameters, with_side_effects=False)

  def workspace_with_side_effects(self,
                                  name: str = None,
                                  parameters: List[WorkspaceParameter] = []
                                  ) -> Callable[[Callable[..., Dict[str, 'State']]], 'Workspace']:
    return self._workspace(name, parameters, with_side_effects=True)

  def trigger_box(self,
                  workspace_name: str,
                  box_id: str,
                  custom_box_stack: List[str] = []):
    '''Triggers the computation of all the GUIDs in the box which is in the
    saved workspace named ``workspace_name`` and has ``boxID=box_id``. If
    custom_box_stack is not empty, it specifies the sequence of custom boxes
    which leads to the workspace which contains the box with the given box_id.
    '''
    return self._send(
        '/ajax/triggerBox',
        dict(workspace=dict(top=workspace_name, customBoxStack=custom_box_stack), box=box_id))


class TableSnapshotSequence:
  '''A snapshot sequence representing a list of table type snapshots in LynxKite.

  Attributes:
    location: the LynxKite root directory this snapshot sequence is stored under.
    cron_str: the Cron format defining the valid timestamps and frequency.'''

  def __init__(self, location: str, cron_str: str) -> None:
    self._location = location
    self.cron_str = cron_str

  def snapshot_name(self, date: datetime.datetime) -> str:
    # TODO: make it timezone independent
    return self._location + '/' + str(date)

  def snapshots(self, from_date: datetime.datetime, to_date: datetime.datetime) -> List[str]:
    # We want to include the from_date if it matches the cron format.
    i = croniter(self.cron_str, from_date - datetime.timedelta(seconds=1))
    t = []
    while True:
      dt = i.get_next(datetime.datetime)
      if dt > to_date:
        break
      t.append(self.snapshot_name(dt))
    return t

  def read_interval(self, lk: LynxKite, from_date: datetime.datetime,
                    to_date: datetime.datetime) -> 'State':
    paths = ','.join(self.snapshots(from_date, to_date))
    return lk.importUnionOfTableSnapshots(paths=paths)

  def read_date(self, lk: LynxKite, date: datetime.datetime) -> 'Box':
    path = self.snapshots(date, date)[0]
    return lk.importSnapshot(path=path)

  def save_to_sequence(self, lk: LynxKite, table_state: str, dt: datetime.datetime) -> None:
    # Assert that dt is valid according to the cron_str format.
    assert _timestamp_is_valid(dt, self.cron_str), "Datetime %s does not match cron format %s." % (
        dt, self.cron_str)
    lk.save_snapshot(self.snapshot_name(dt), table_state)


class State:
  '''Represents a named output plug of a box.

  It can recursively store the boxes which are connected to the input plugs of
  the box of this state.
  '''

  def __init__(self, box: 'Box', output_plug_name: str) -> None:
    self.output_plug_name = output_plug_name
    self.box = box

  def __getattr__(self, name: str) -> Callable:

    def f(**kwargs):
      inputs = self.box.bc.inputs(name)
      # This chaining syntax only allowed for boxes with exactly one input.
      assert len(inputs) > 0, '{} does not have an input'.format(name)
      assert len(inputs) < 2, '{} has more than one input'.format(name)
      [input_name] = inputs
      return _new_box(
          self.box.bc, self.box.lk, name, inputs={input_name: self}, parameters=kwargs)

    if not name in self.box.bc.box_names():
      raise AttributeError('{} is not defined on {}'.format(name, self))
    return f

  def __dir__(self) -> Iterable[str]:
    return itertools.chain(super().__dir__(), self.box.bc.box_names())

  def __str__(self) -> str:
    return "Output {} of box {}".format(self.output_plug_name, self.box)

  def sql(self, sql: str, **kwargs) -> 'SingleOutputAtomicBox':
    return self.sql1(sql=sql, **kwargs)

  def df(self, limit: int = -1):
    '''Returns a Pandas DataFrame if this state is a table.'''
    import pandas
    table = self.get_table_data(limit)
    header = [c.name for c in table.header]
    data = [[getattr(c, 'double', c.string) if c.defined else None for c in r] for r in table.data]
    return pandas.DataFrame(data, columns=header)

  def get_table_data(self, limit: int = -1) -> types.SimpleNamespace:
    '''Returns the "raw" table data if this state is a table.'''
    return self.box.lk.get_table_data(self.box.lk.get_state_id(self), limit)

  def get_project(self) -> types.SimpleNamespace:
    '''Returns the project metadata if this state is a project.'''
    return self.box.lk.get_project(self.box.lk.get_state_id(self))

  def run_export(self) -> str:
    '''Triggers the export if this state is an ``exportResult``.

    Returns the prefixed path of the exported file.
    '''
    lk = self.box.lk
    state_id = lk.get_state_id(self)
    export = lk.get_export_result(state_id)
    if export.result.computeProgress != 1:
      scalar = lk.get_scalar(export.result.id)
      assert scalar.string == 'Export done.', scalar.string
      export = lk.get_export_result(state_id)
      assert export.result.computeProgress == 1, 'Failed to compute export result scalar.'
    return export.parameters.path

  def compute(self) -> None:
    '''Triggers the computation of this state.

    Uses a temporary folder to save a temporary workspace for this computation.
    '''
    folder = _random_ws_folder()
    name = 'tmp_ws_name'
    full_path = folder + '/' + name
    box = self.computeInputs()
    lk = self.box.lk
    ws = Workspace(name, [box])
    lk.save_workspace_recursively(ws, folder)
    lk.trigger_box(full_path, 'box_0')
    lk.remove_name(folder, force=True)

  def save_snapshot(self, path: str) -> None:
    '''Save this state as a snapshot under path.'''
    lk = self.box.lk
    state_id = lk.get_state_id(self)
    lk.save_snapshot(path, state_id)

  def save_to_sequence(self, tss: TableSnapshotSequence, date: datetime.datetime) -> None:
    '''Save this state to the ``tss`` TableSnapshotSequence with ``date`` as
    the date of the snapshot.'''
    lk = self.box.lk
    state_id = lk.get_state_id(self)
    tss.save_to_sequence(lk, state_id, date)


class Box:
  '''Represents a box in a workspace segment.

  It can store workspace segments, connected to its input plugs.
  '''

  def __init__(self, box_catalog: BoxCatalog, lk: LynxKite,
               inputs: Dict[str, State], parameters: Dict[str, Any]) -> None:
    self.bc = box_catalog
    self.lk = lk
    self.inputs = inputs
    self.parameters: Dict[str, str] = {}
    self.parametric_parameters: Dict[str, str] = {}
    self.outputs: Set[str] = set()
    # We separate normal and parametric parameters here.
    # Parametric parameters can be specified as `name=PP('parametric value')`
    for key, value in parameters.items():
      if isinstance(value, ParametricParameter):
        self.parametric_parameters[key] = str(value)
      else:
        self.parameters[key] = str(value)

  def _operationId(self, workspace_root: str) -> str:
    '''The id that we send to the backend to identify a box.'''
    raise NotImplementedError()

  def name(self) -> str:
    '''Either the name in the box catalog or the name under which the box is saved.'''
    raise NotImplementedError()

  def to_json(self, id_resolver: Callable[['Box'], str], workspace_root: str) -> SerializedBox:
    '''Creates the json representation of a box in a workspace.

    The inputs have to be connected, and all the attributes have to be
    defined when we call this.
    '''
    def input_state(state):
      return {'boxId': id_resolver(state.box), 'id': state.output_plug_name}

    operationId = self._operationId(workspace_root)
    return SerializedBox({
        'id': id_resolver(self),
        'operationId': operationId,
        'parameters': self.parameters,
        'x': 0, 'y': 0,
        'inputs': {plug: input_state(state) for plug, state in self.inputs.items()},
        'parametricParameters': self.parametric_parameters})

  def register(self, side_effect_collector):
    side_effect_collector.add_box(self)

  def __getitem__(self, index: str) -> State:
    if index not in self.outputs:
      raise KeyError(index)
    return State(self, index)


class AtomicBox(Box):
  '''
  An ``AtomicBox`` is a ``Box`` that can not be further decomposed. It corresponds to a single
  frontend operation.
  '''

  def __init__(self, box_catalog: BoxCatalog, lk: LynxKite, operation: str,
               inputs: Dict[str, State], parameters: Dict[str, Any]) -> None:
    super().__init__(box_catalog, lk, inputs, parameters)
    self.operation = operation
    self.outputs = set(self.bc.outputs(operation))
    exp_inputs = set(self.bc.inputs(operation))
    got_inputs = inputs.keys()
    assert got_inputs == exp_inputs, 'Got box inputs: {}. Expected: {}'.format(
        got_inputs, exp_inputs)

  def _operationId(self, workspace_root):
    return self.bc.operation_id(self.operation)

  def name(self):
    return self.operation

  def __str__(self) -> str:
    return "Operation {} with parameters {} and inputs {}".format(
        self.operation,
        self.parameters,
        self.inputs)


class SingleOutputAtomicBox(AtomicBox, State):
  '''
  An ``AtomicBox`` with a single output. This makes chaining multiple operations after each other
  possible.
  '''

  def __init__(self, box_catalog: BoxCatalog, lk: LynxKite, operation: str,
               inputs: Dict[str, State], parameters: Dict[str, Any], output_name: str) -> None:
    AtomicBox.__init__(self, box_catalog, lk, operation, inputs, parameters)
    State.__init__(self, self, output_name)


class CustomBox(Box):
  '''
  A ``CustomBox`` is a ``Box`` composed of multiple other boxes.
  '''

  def __init__(self, box_catalog: BoxCatalog, lk: LynxKite, workspace: 'Workspace',
               inputs: Dict[str, State], parameters: Dict[str, Any]) -> None:
    super().__init__(box_catalog, lk, inputs, parameters)
    self.workspace = workspace
    self.outputs = set(workspace.outputs())
    exp_inputs = set(workspace.inputs())
    got_inputs = inputs.keys()
    assert got_inputs == exp_inputs, 'Got box inputs: {}. Expected: {}'.format(
        got_inputs, exp_inputs)

  def _operationId(self, workspace_root):
    return _normalize_path(workspace_root + '/' + self.name())

  def name(self):
    return self.workspace.name()

  def __str__(self) -> str:
    return "Custom box {} with parameters {} and inputs {}".format(
        self.name(),
        self.parameters,
        self.inputs)


class SingleOutputCustomBox(CustomBox, State):
  '''
  An ``CustomBox`` with a single output. This makes chaining multiple operations after each other
  possible.
  '''

  def __init__(self, box_catalog: BoxCatalog, lk: LynxKite, workspace: 'Workspace',
               inputs: Dict[str, State], parameters: Dict[str, Any], output_name: str) -> None:
    CustomBox.__init__(self, box_catalog, lk, workspace, inputs, parameters)
    State.__init__(self, self, output_name)


def _python_name(name: str) -> str:
  '''Transforms a space separated string into a camelCase format.

  The operation "Use base project as segmentation" will be called as
  ``useBaseProjectAsSegmentation``. Dashes are ommitted.
  '''
  name = ''.join([c if c.isalnum() or c == ' ' else '' for c in name])
  return ''.join(
      [x.lower() for x in name.split()][:1] +
      [x[:1].upper() + x[1:] for x in name.split()][1:])


_anchor_box = SerializedBox({
    'id': 'anchor',
    'operationId': 'Anchor',
    'parameters': {},
    'x': 0, 'y': 0,
    'inputs': {},
    'parametricParameters': {},
})


def _new_box(bc: BoxCatalog, lk: LynxKite, operation: Union[str, 'Workspace'],
             inputs: Dict[str, State], parameters: Dict[str, Any]) -> Box:
  if isinstance(operation, str):
    outputs = bc.outputs(operation)
    if len(outputs) == 1:
      return SingleOutputAtomicBox(bc, lk, operation, inputs, parameters, outputs[0])
    else:
      return AtomicBox(bc, lk, operation, inputs, parameters)
  else:
    outputs = operation.outputs()
    if len(outputs) == 1:
      return SingleOutputCustomBox(bc, lk, operation, inputs, parameters, outputs[0])
    else:
      return CustomBox(bc, lk, operation, inputs, parameters)


def _reverse_bfs_on_boxes(roots: List[Box], list_roots: bool = True) -> Iterator[Box]:
  '''
  Lists all the dependencies of boxes in ``roots`` in a bfs way.
  '''
  to_process = collections.deque(roots)
  if list_roots:
    for box in roots:
      yield box
  processed = set(roots)
  while len(to_process):
    box = to_process.pop()
    for input_state in box.inputs.values():
      parent_box = input_state.box
      if parent_box not in processed:
        processed.add(parent_box)
        to_process.append(parent_box)
        yield parent_box


def _atomic_source_of_state(box_list, state) -> 'BoxPath':
  '''`state` is an output state of the last box of `box_list`.'''
  source_box = state.box
  if isinstance(source_box, AtomicBox):
    return BoxPath(source_box, box_list)
  else:  # we need the proper output box of the custom box
    output_box = [box for box in source_box.workspace.output_boxes()
                  if box.parameters['name'] == state.output_plug_name][0]
    return BoxPath(output_box, box_list + [source_box])


class Endpoint:
  '''Represents an automation endpoint in a workspace.

  Endpoints used in automation to trigger different parts of a pipeline. There
  are three different types of endpoint: input, output and side effect
  (typically exports).
  '''

  def __init__(self, box_path: 'BoxPath') -> None:
    self.box_path = box_path
    self.ntap = box_path.non_trivial_parent_of_endpoint()

  def to_dict(self):
    return self.box_path.to_dict()


class BoxPath:
  '''Represents a box (which can be inside (nested) custom boxes).
  It can be used for example to trigger boxes inside custom boxes.

  ``stack[i+1]`` is always a box contained by the workspace referred by the
  custom box ``stack[i]`` and  ``base`` is a box contained by ``stack[-1]``.
  '''

  def __init__(self, base: AtomicBox, stack: List[CustomBox] = []) -> None:
    self.base = base
    self.stack = stack

  def __str__(self) -> str:
    return ' -> '.join([b.name() for b in cast(List[Box], self.stack) + [cast(Box, self.base)]])

  def add_box_as_prefix(self, box: CustomBox) -> 'BoxPath':
    return BoxPath(self.base, [box] + self.stack)

  def atomic_box(self) -> AtomicBox:
    return self.base

  def custom_box_stack(self) -> List[CustomBox]:
    return self.stack

  def parent(self, input_name) -> 'BoxPath':
    parent_state = self.base.inputs[input_name]
    return _atomic_source_of_state(self.custom_box_stack(), parent_state)

  def parents(self) -> List['BoxPath']:
    box = self.base
    if box.inputs:  # normal box with inputs
      return [self.parent(inp) for inp in box.inputs.keys()]
    elif box.operation == 'input':  # input box
      if not self.stack:  # top level input box
        return [FakeBoxPathForInputParent(box.parameters['name'])]
      else:  # input box of a nested custom box
        containing_custom_box = self.stack[-1]
        input_name = box.parameters['name']
        source_state = containing_custom_box.inputs[input_name]
        return [_atomic_source_of_state(self.stack[:-1], source_state)]
    else:  # no parents
      return []

  def non_trivial_parent_of_endpoint(self) -> 'BoxPath':
    '''Computes the first  non-trivial atomic upstream BoxPath for this BoxPath.

    It can only be used on "endpoints" (triggerables with no output or top level inputs).
    The first non-trivial upstream box is either a top-level input box, or
    a box which computes something (so not an input box or an output box).
    For top level inputs it is a fake box path, to unify the handling of endpoints.
    '''
    def trivial(box_path) -> bool:
      if isinstance(box_path, FakeBoxPathForInputParent):
        return False
      box = box_path.base
      return box.operation == 'output' or (box.operation == 'input' and box_path.stack)

    box = self.base
    parents = self.parents()
    assert len(parents) == 1, 'More than one parent.'
    parent = self.parents()[0]
    while trivial(parent):
      parents = parent.parents()
      assert len(parents) == 1, 'More than one parent.'
      parent = parents[0]

    return parent

  def to_dict(self):
    '''Returns a (human readable) dict representation of this object.'''
    parent = None
    op = self.base.operation
    op_param = self.base.parameters
    if self.stack:
      parent = self.stack[-1].name()
    return dict(operation=op, params=op_param, nested_in=parent)

  def __hash__(self):
    return hash(','.join([str(box) for box in self.stack] + [str(self.base)]))

  def __eq__(self, other):
    return self.base == other.base and self.stack == other.stack


class FakeBoxPathForInputParent(BoxPath):
  '''For top level inputs we create a fake parent. With this, we can unify the
  dependency computation of endpoints.
  '''

  def __init__(self, input_name) -> None:
    self.stack = []
    self.name = input_name

  @property
  def base(self):
    raise Exception('This is just a input parent fake box.')

  def add_box_as_prefix(self, box):
    raise Exception('This is just a input parent fake box.')

  def atomic_box(self):
    raise Exception('This is just a input parent fake box.')

  def parent(self, input_name) -> 'BoxPath':
    raise Exception('This is just a input parent fake box.')

  def parents(self) -> List['BoxPath']:
    return []

  def non_trivial_parent_of_endpoint(self) -> 'BoxPath':
    raise Exception('This is just a input parent fake box.')

  def to_dict(self):
    return dict(
        operation='fake input parent',
        params=dict(name=self.name),
        nested_in=None)

  def __hash__(self):
    return hash(f'fake input box {self.name}')

  def __eq__(self, other):
    return isinstance(other, FakeBoxPathForInputParent) and self.name == other.name


class SideEffectCollector:
  def __init__(self):
    self.direct_children: List[Box] = []
    self.childrens_triggerables: List[BoxPath] = []

  def all_triggerables(self) -> Iterable[BoxPath]:
    yield from self.childrens_triggerables
    for box in self.direct_children:
      yield from self._all_triggerables_in_box(box)

  @staticmethod
  def _all_triggerables_in_box(box):
    if isinstance(box, AtomicBox):
      yield BoxPath(box)
    elif isinstance(box, CustomBox):
      for triggerable in box.workspace.trigger_paths():
        yield triggerable.add_box_as_prefix(box)

  def add_box(self, box: Box) -> None:
    self.direct_children.append(box)

  def __str__(self):
    btb = 'To build ==> ' + str([b.name() for b in self.direct_children])
    btt = ' To trigger ==> ' + str([btt for btt in self.all_triggerables()])
    return btb + btt


class Workspace:
  '''Immutable class representing a LynxKite workspace.'''

  def __init__(self, name: str,
               terminal_boxes: List[Box],
               trigger_paths: List[BoxPath] = [],
               input_boxes: List[AtomicBox] = [],
               ws_parameters: List[WorkspaceParameter] = []) -> None:
    self._name = name or 'Anonymous'
    self._all_boxes: Set[Box] = set()
    self._box_ids: Dict[Box, str] = dict()
    self._next_id = 0
    assert all(b.operation == 'input' for b in input_boxes), 'Non-input box in input_boxes'
    self._inputs = [inp.parameters['name'] for inp in input_boxes]
    self._output_boxes = [box for box in terminal_boxes
                          if isinstance(box, AtomicBox) and box.operation == 'output']
    self._outputs = [outp.parameters['name'] for outp in self._output_boxes]
    self._ws_parameters = ws_parameters
    self._trigger_paths = trigger_paths
    self._input_boxes = input_boxes
    self._terminal_boxes = terminal_boxes
    self._bc = self._terminal_boxes[0].bc
    self._lk = self._terminal_boxes[0].lk
    for box in _reverse_bfs_on_boxes(self._terminal_boxes):
      self._add_box(box)

  def _add_box(self, box):
    self._all_boxes.add(box)
    self._box_ids[box] = "box_{}".format(self._next_id)
    self._next_id += 1

  def id_of(self, box: Box) -> str:
    return self._box_ids[box]

  def _box_to_trigger_to_box_ids(self, box_to_trigger: BoxPath) -> List[str]:
    '''Converts a BoxPath object to the list of corresponding box ids in this Workspace.'''
    box_ids: List[str] = []
    outer_ws = self
    for box in box_to_trigger.stack:
      box_ids.append(outer_ws.id_of(box))
      outer_ws = box.workspace
    box_ids.append(outer_ws.id_of(box_to_trigger.base))
    return box_ids

  def _ws_parameters_to_str(self):
    return json.dumps([param.to_json() for param in self._ws_parameters])

  def to_json(self, workspace_root: str) -> List[SerializedBox]:
    non_anchor_boxes = [
        box.to_json(self.id_of, workspace_root) for box in self._all_boxes]
    # We use ws_parameters to customize _anchor_box.
    ab = copy.deepcopy(_anchor_box)
    ab['parameters'] = dict(parameters=self._ws_parameters_to_str())
    return [ab] + non_anchor_boxes

  def required_workspaces(self) -> List['Workspace']:
    return [
        box.workspace for box in self._all_boxes
        if isinstance(box, CustomBox)]

  def inputs(self) -> List[str]:
    return list(self._inputs)

  def outputs(self) -> List[str]:
    return list(self._outputs)

  def output_boxes(self) -> List[AtomicBox]:
    return self._output_boxes

  def name(self) -> str:
    return self._name

  def trigger_paths(self) -> List[BoxPath]:
    return self._trigger_paths

  def has_date_parameter(self) -> bool:
    return 'date' in [p.name for p in self._ws_parameters]

  def terminal_box_ids(self) -> List[str]:
    return [self.id_of(box) for box in self._terminal_boxes]

  def __call__(self, *args, **kwargs) -> Box:
    inputs = dict(zip(self.inputs(), args))
    return _new_box(self._bc, self._lk, self, inputs=inputs, parameters=kwargs)

  def triggerable_boxes(self) -> List[AtomicBox]:
    # TODO: replace it with real list of triggerables, collected in the
    # side effect collector of the workspace.
    return self._output_boxes

  def automation_endpoints(self) -> List[Endpoint]:
    '''Returns the endpoints, relevant in automation.

    The endpoints correspond to top level input boxes, top level output boxes and
    side effect boxes.
    '''
    inputs = [Endpoint(BoxPath(inp)) for inp in self._input_boxes]
    outputs = [Endpoint(BoxPath(outp)) for outp in self._output_boxes]
    side_effects = [Endpoint(se) for se in self._trigger_paths]
    return inputs + outputs + side_effects

  def automation_dependencies(self) -> Dict[Endpoint, Set[Endpoint]]:
    endpoints = self.automation_endpoints()
    # One NTAP (non-trivial atomic parent) can belong to multiple endpoints
    ntap_to_endpoints: Dict[BoxPath, Set[Endpoint]] = collections.defaultdict(set)
    for ep in endpoints:
      ntap_to_endpoints[ep.ntap].add(ep)
    endpoint_dependencies: Dict[Endpoint, Set[Endpoint]] = collections.defaultdict(set)
    for ep in endpoints:
      to_process = collections.deque(ep.ntap.parents())
      visited: Set[BoxPath] = set()
      while to_process:
        box_path = to_process.pop()
        visited.add(box_path)
        if box_path in ntap_to_endpoints.keys():
          endpoint_dependencies[ep].update(ntap_to_endpoints.get(box_path, []))
        to_process.extend([bp for bp in box_path.parents() if not bp in visited])
    return endpoint_dependencies

  def _trigger_box(self, box_to_trigger: BoxPath, full_path: str):
    lk = self._lk
    box_ids = self._box_to_trigger_to_box_ids(box_to_trigger)
    # The last id is a "normal" box id, the rest are the custom box stack.
    lk.trigger_box(full_path, box_ids[-1], box_ids[:-1])

  def save(self, saved_under_folder: str) -> Tuple[str, str]:
    lk = self._lk
    return lk.save_workspace_recursively(self, saved_under_folder)

  def trigger_saved(self, box_to_trigger: BoxPath, saved_under_folder: str):
    ''' Triggers one side effect.

    Assumes the workspace is saved under `saved_under_root`.
    '''
    full_path = _normalize_path(saved_under_folder + '/' + self._name)
    self._trigger_box(box_to_trigger, full_path)

  def trigger(self, box_to_trigger: BoxPath):
    ''' Triggers one side effect.

    Assumes the workspace is not saved, so saves it under a random folder.
    '''
    random_folder = _random_ws_folder()
    _, full_path = self.save(random_folder)
    self._trigger_box(box_to_trigger, full_path)

  def trigger_all_side_effects(self):
    ''' Triggers all side effects.

    Also saves the workspace under a temporary folder.
    '''
    temporary_folder = _random_ws_folder()
    self.save(temporary_folder)
    for btt in self._trigger_paths:
      self.trigger_saved(btt, temporary_folder)


class InputRecipe:
  '''Base class for input recipes.

  Can check whether an input is available, and can build a workspace segment which
  loads the input into a workspace.
  '''

  def is_ready(self, lk: LynxKite, date: datetime.datetime) -> bool:
    raise NotImplementedError()

  def build_boxes(self, lk: LynxKite, date: datetime.datetime) -> State:
    raise NotImplementedError()

  def validate(self, date: datetime.datetime) -> None:
    raise NotImplementedError()


class TableSnapshotRecipe(InputRecipe):
  '''Input recipe for a table snapshot sequence.
     @param: tss: The TableSnapshotSequence used by this recipe. Can be None, but has to be
             set via set_tss before using this class.
     @param: delta: Steps back delta in time according to the cron string of the tss. Optional,
             if not set this recipe uses the date parameter.'''

  def __init__(self, tss: TableSnapshotSequence = None, delta: int = 0) -> None:
    self.tss = tss
    self.delta = delta

  def set_tss(self, tss: TableSnapshotSequence) -> None:
    assert self.tss is None
    self.tss = tss

  def validate(self, date: datetime.datetime) -> None:
    assert self.tss, 'TableSnapshotSequence needs to be set.'
    assert _timestamp_is_valid(
        date, self.tss.cron_str), '{} does not match {}.'.format(date, self.tss.cron_str)

  def is_ready(self, lk: LynxKite, date: datetime.datetime) -> bool:
    self.validate(date)
    adjusted_date = _step_back(self.tss.cron_str, date, self.delta)
    r = lk.get_directory_entry(self.tss.snapshot_name(adjusted_date))
    return r.exists and r.isSnapshot

  def build_boxes(self, lk: LynxKite, date: datetime.datetime) -> State:
    self.validate(date)
    adjusted_date = _step_back(self.tss.cron_str, date, self.delta)
    return self.tss.read_interval(lk, adjusted_date, adjusted_date)


class RecipeWithDefault(InputRecipe):
  '''Input recipe with a default value.
     @param: src_recipe: The source recipe to use if possible.
     @param: default_date: Provide the default box for this date and src_recipe for later dates.
     @param: default_state: Provide this State for dates earlier than the default date.'''

  def __init__(self, src_recipe: InputRecipe, default_date: datetime.datetime,
               default_state: State) -> None:
    self.src_recipe = src_recipe
    self.default_date = default_date
    self.default_state = default_state

  def validate(self, date: datetime.datetime) -> None:
    if date != self.default_date:
      self.src_recipe.validate(date)

  def is_ready(self, lk: LynxKite, date: datetime.datetime) -> bool:
    self.validate(date)
    return date == self.default_date or self.src_recipe.is_ready(lk, date)

  def build_boxes(self, lk: LynxKite, date: datetime.datetime) -> State:
    self.validate(date)
    if date == self.default_date:
      return self.default_state
    else:
      return self.src_recipe.build_boxes(lk, date)


class WorkspaceSequence:
  '''Represents a workspace sequence.

  It can be used in automation to create instances of a workspace for
  timestamps, wrapped in a workspace which can get inputs, and saves outputs.
  '''

  def __init__(self, ws: Workspace, schedule: str, start_date: datetime.datetime,
               params: Dict[str, Any], lk_root: str, dfs_root: str,
               input_recipes: List[InputRecipe]) -> None:
    self._ws = ws
    self._schedule = schedule
    self._start_date = start_date
    self._params = params
    self._lk_root = lk_root
    self._dfs_root = dfs_root
    self._input_names = self._ws.inputs()  # For the order of the inputs
    self._input_recipes = dict(zip(self._input_names, input_recipes))
    self._input_sequences: Dict[str, TableSnapshotSequence] = {}
    for inp in self._input_names:
      location = _normalize_path(self._lk_root + '/inputs/' + inp)
      self._input_sequences[inp] = TableSnapshotSequence(location, self._schedule)
    self._output_sequences: Dict[str, TableSnapshotSequence] = {}
    for output in self._ws.outputs():
      location = _normalize_path(self._lk_root + '/outputs/' + output)
      self._output_sequences[output] = TableSnapshotSequence(location, self._schedule)

  def output_sequences(self) -> Dict[str, TableSnapshotSequence]:
    '''Returns the output sequences of the workspace sequence as a dict.'''
    return self._output_sequences

  def input_sequences(self) -> Dict[str, TableSnapshotSequence]:
    '''Returns the input sequences of the workspace sequence as a dict.'''
    return self._input_sequences

  def input_names(self):
    ''' The sorted list of the input names of the wrapped workspace.'''
    return self._input_names

  def input_recipes(self):
    ''' Dict of input recipes, the keys are names.'''
    return self._input_recipes

  def ws_for_date(self, lk: LynxKite, date: datetime.datetime) -> 'WorkspaceSequenceInstance':
    '''If the wrapped ws has a ``date`` workspace parameter, then we will use the
    ``date`` parameter of this method as a value to pass to the workspace. '''
    assert date >= self._start_date, "{} preceeds start date = {}".format(date, self._start_date)
    assert _timestamp_is_valid(
        date, self._schedule), "{} is not valid according to {}".format(date, self._schedule)
    return WorkspaceSequenceInstance(self, lk, date)

  def lk_root(self) -> str:
    return self._lk_root


class WorkspaceSequenceInstance:

  def __init__(self, wss: WorkspaceSequence, lk: LynxKite, date: datetime.datetime) -> None:
    self._wss = wss
    self._lk = lk
    self._date = date

  def wrapper_name(self) -> str:
    return 'wrapper_for_{}'.format(self._date)

  def folder_name(self) -> str:
    return 'workspaces_for_{}'.format(self._date)

  def wrapper_folder_name(self) -> str:
    return '/'.join([self._wss.lk_root(), 'workspaces', self.folder_name()])

  def full_name(self) -> str:
    name = '/'.join([self.wrapper_folder_name(), self.wrapper_name()])
    return _normalize_path(name)

  def is_saved(self) -> bool:
    path = self.full_name()
    r = self._lk.get_directory_entry(path)
    return r.exists and r.isWorkspace

  def wrapper_ws(self) -> Workspace:
    lk = self._lk

    @lk.workspace_with_side_effects(name=self.wrapper_name())
    def ws_instance(se_collector):
      inputs = [
          self._lk.importSnapshot(
              path=self._wss.input_sequences()[input_name].snapshot_name(
                  self._date))
          for input_name in self._wss.input_names()]
      ws_as_box = (
          self._wss._ws(*inputs, **self._wss._params, date=self._date) if self._wss._ws.has_date_parameter()
          else self._wss._ws(*inputs, **self._wss._params))
      ws_as_box.register(se_collector)
      for output in self._wss._ws.outputs():
        out_path = self._wss.output_sequences()[output].snapshot_name(self._date)
        ws_as_box[output].saveToSnapshot(path=out_path).register(se_collector)

    return ws_instance

  def save(self) -> None:
    assert not self.is_saved(), 'WorkspaceSequenceInstance is already saved.'
    ws = self.wrapper_ws()
    self._lk.save_workspace_recursively(ws, self.wrapper_folder_name())

  def run_input(self, input_name):
    ws_name = f'Workspace for {self._date}'
    lk = self._lk

    @lk.workspace_with_side_effects(name=ws_name)
    def input_ws(se_collector):
      input_state = self._wss.input_recipes()[input_name].build_boxes(self._lk, self._date)
      input_state.saveToSnapshot(path=(self._wss.input_sequences()[input_name]
                                       .snapshot_name(self._date))).register(se_collector)

    folder = _normalize_path('/'.join([self._wss.lk_root(), 'input workspaces', input_name]))
    input_ws.save(folder)
    input_ws.trigger_all_side_effects()

  def run_all_inputs(self):
    for input_name in self._wss.input_names():
      self.run_input(input_name)

  def run(self) -> None:
    '''We trigger all the side effect boxes of the ws.

    This means all the side effects in the wrapped ws and the saving of
    the outputs of the wrapped ws.'''
    if not self.is_saved():  # WorkspaceSequenceInstance has to be saved to be able to run.
      self.save()
    # Compute the inputs.
    self.run_all_inputs()
    saved_under_folder = self.wrapper_folder_name()
    # We assume that the same box ids will be generated every time
    # we regenerate this workspace.
    ws = self.wrapper_ws()
    for btt in ws.trigger_paths():
      ws.trigger_saved(btt, saved_under_folder)


T = TypeVar('T')


def _topological_sort(dependencies: Dict[T, Set[T]]) -> Iterable[Set[T]]:
  '''
  Returns groups of Ts in a way, that elements in group_i depend only on elements in
  group_j where j < i.

  dependencies[x] = set(), if x does not depend on anything
  '''
  deps = dependencies
  while True:
    next_group = set(box_id for box_id, dep in deps.items() if len(dep) == 0)
    if not next_group:
      break
    yield next_group
    deps = {box_id: dep - next_group for box_id, dep in deps.items() if box_id not in next_group}


def _layout(boxes: List[SerializedBox]) -> List[SerializedBox]:
  '''Compute coordinates of boxes in a workspace.'''
  dx = 200
  dy = 200
  ox = 150
  oy = 150

  dependencies: Dict[str, Set[str]] = {box['id']: set() for box in boxes}
  level = {}
  for box in boxes:
    current_box = box['id']
    for inp in box['inputs'].values():
      input_box = inp['boxId']
      dependencies[current_box].add(input_box)

  cur_level = 0
  groups = list(_topological_sort(dependencies))
  for group in groups:
    for box_id in group:
      level[box_id] = cur_level
    cur_level = cur_level + 1

  num_boxes_on_level = [0] * (len(groups) + 1)
  boxes_with_coordinates = []
  for box in boxes:
    if box['id'] != 'anchor':
      box_level = level[box['id']]
      box['x'] = ox + box_level * dx
      box['y'] = oy + num_boxes_on_level[box_level] * dy
      num_boxes_on_level[box_level] = num_boxes_on_level[box_level] + 1
    boxes_with_coordinates.append(box)
  return boxes_with_coordinates


def _minimal_dag(g: Dict[T, Set[T]]) -> Dict[T, Set[T]]:
  '''
  This function creates another dependency graph which has the same implicit dependencies
  as the original one (if a depends on b and b depends on c, a implicitly depends on c) but
  is minimal for edge exclusion, i.e. with any explicit dependency deleted the resulting
  graph will miss some implicit or explicit dependency from the original graph.

  Formally:
  g = (V, E)
  g' = (V, E')
  If g' = _minimal_dag(g) then
    - TC(g') = TC(g)
    - ∀ e ∈ E: TC((V, E' - e)) != TC(g)
  where TC(g) (the transitive closure of g) is defined as (V, E*) and
  ∀ (v, v') ∈ VxV: (v, v') ∈ E* ⇔ there is a directed path from v to v' in g
  '''
  transitive_closure: Dict[T, Set[T]] = dict()
  for group in _topological_sort(g):
    for elem in group:
      deps = g[elem]
      transitive_closure[elem] = deps
      for d in deps:
        transitive_closure[elem] = transitive_closure[elem] | transitive_closure[d]
  min_dag: Dict[T, Set[T]] = {n: set() for n in g}
  for n in g:
    deps = g[n]
    for m in deps:
      is_direct_dependency = True
      for o in deps:
        if m in transitive_closure[o]:
          is_direct_dependency = False
          break
      if is_direct_dependency:
        min_dag[n].add(m)
  return min_dag


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
