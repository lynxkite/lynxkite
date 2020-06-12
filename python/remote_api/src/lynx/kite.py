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
    lk.createExampleGraph().sql('select * from graph_attributes').df()
'''
import copy
import functools
import json
import os
import random
import sys
import types
import datetime
import inspect
import re
import itertools
from collections import deque, defaultdict, Counter
from typing import (Dict, List, Union, Callable, Any, Tuple, Iterable, Set, NewType, Iterator,
                    TypeVar, Optional, Collection)
import requests
from tempfile import NamedTemporaryFile, TemporaryDirectory, mkstemp
import textwrap
import shutil


if sys.version_info.major < 3 or (sys.version_info.major == 3 and sys.version_info.minor < 6):
  raise Exception('At least Python version 3.6 is needed!')


def random_filename() -> str:
  return ''.join(random.choices('0123456789ABCDEF', k=16))


def _random_ws_folder() -> str:
  return 'tmp_workspaces/{}'.format(random_filename())


def _normalize_path(path: str) -> str:
  '''Removes leading, trailing slashes and slash-duplicates.'''
  return re.sub('/+', '/', path).strip('/')


def _assert_lk_success(output, box_id, plug):
  '''Asserts that the output is enabled and raises a LynxException otherwise.'''
  if not output.success.enabled:
    msg = f'Output `{plug}` of `{box_id}` has failed: {output.success.disabledReason}'
    raise LynxException(msg)


def escape(s: Union[str, 'ParametricParameter']) -> Union[str, 'ParametricParameter']:
  '''Sanitizes a string for injecting into a generated SQL query.'''
  sql = str(s)
  escaped = sql.replace('\\', '\\\\').replace('"', '\\"').replace("'", "\\'")
  return ParametricParameter(escaped) if isinstance(s, ParametricParameter) else escaped


def dedent(s: Union[str, 'ParametricParameter']) -> Union[str, 'ParametricParameter']:
  '''Format sql queries to look nicer on the UI.'''
  sql = str(s)
  nicer_sql = textwrap.dedent(sql.lstrip('\n'))
  return ParametricParameter(nicer_sql) if isinstance(s, ParametricParameter) else nicer_sql


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


def _to_outputs(returned):
  '''Turns the return value of a workspace builder function into a list of output boxes.'''
  if isinstance(returned, State):
    return [returned.output(name='output')]
  elif returned is None:
    return []
  else:
    return [state.output(name=name) for name, state in returned.items()]


def map_args(
        signature: inspect.Signature, bound: inspect.BoundArguments,
        map_fn: Callable[[str, Any], Any]) -> inspect.BoundArguments:
  # Create a copy so we don't modify the original.
  bound = signature.bind(*bound.args, **bound.kwargs)
  for k, v in bound.arguments.items():
    p = signature.parameters[k]
    if p.kind == inspect.Parameter.VAR_POSITIONAL:  # This is *args.
      v = list(v)  # It's a tuple normally. But we want to mutate it.
      for i, value in enumerate(v):
        v[i] = map_fn(f'{k}_{i + 1}', value)
      bound.arguments[k] = v
    elif p.kind == inspect.Parameter.VAR_KEYWORD:  # This is **kwargs.
      for name, value in v.items():
        v[name] = map_fn(f'{k}_{name}', value)
    else:  # Normal positional or keyword argument.
      bound.arguments[k] = map_fn(k, v)
  return bound


def subworkspace(fn: Callable):
  '''Allows using the decorated function as a LynxKite custom box.

  Example use::

    @subworkspace
    def my_func(input1, other_arg1):
      return input1.sql1(sql=f'select {other_arg1} from vertices')

    df = my_func(lk.createExampleGraph(), 'name').df()

  To make a multi-output box, return a dictionary instead of a state.

  ``my_func()`` can have any number of positional or keyword arguments. The arguments carrying
  states will be turned into the inputs of the custom box.

  Use ``@ws_param`` to take workspace parameters.

  To define a custom box with side-effects, take an argument with a default value of
  SideEffectCollector.AUTO. A fresh SideEffectCollector() will be provided each time the function is
  called. ::

    @subworkspace
    def my_func(input1, sec=SideEffectCollector.AUTO):
      input1.saveAsSnapshot('x').register(sec)

    my_func(lk.createExampleGraph()).trigger()
  '''
  signature = inspect.signature(fn, follow_wrapped=False)
  secs = [p.name for p in signature.parameters.values()
          if p.default is SideEffectCollector.AUTO]
  assert len(secs) <= 1, f'More than one SideEffectCollector parameters found for {fn}'

  @functools.wraps(fn)
  def wrapper(*args, _ws_params: List[WorkspaceParameter] = [], _ws_name: str = None, **kwargs):
    # create a new signature on every call since we will bind different arguments per call
    signature = inspect.signature(fn, follow_wrapped=False)
    # Separate workspace parameters from the normal Python parameters.
    ws_param_bindings = {wp.name: kwargs[wp.name] for wp in _ws_params if wp.name in kwargs}
    for wp in _ws_params:
      if wp.name in signature.parameters:
        kwargs[wp.name] = pp('$' + wp.name)
      elif wp.name in kwargs:
        del kwargs[wp.name]

    # Replace states with input boxes.
    input_states = []
    input_boxes = []

    def map_param(name, value):
      if isinstance(value, State):
        b = value.box.lk.input(name=name)
        input_states.append(value)
        input_boxes.append(b)
        return b
      else:
        return value
    manual_box_id = kwargs.pop('_id', None)
    bound = signature.bind(*args, **kwargs)
    for k in bound.arguments:
      assert k not in secs, f'Explicitly set SideEffectCollector parameter for {fn}'
    bound = map_args(signature, bound, map_param)
    sec = SideEffectCollector()
    if secs:
      bound.arguments[secs[0]] = sec

    # Build the workspace.
    outputs = _to_outputs(fn(*bound.args, **bound.kwargs))
    ws = Workspace(
        terminal_boxes=outputs + sec.top_level_side_effects,
        side_effect_paths=list(sec.all_triggerables()),
        input_boxes=input_boxes,
        ws_parameters=_ws_params,
        custom_box_id_base=_ws_name if _ws_name is not None else fn.__name__)

    # Return the custom box.
    return ws(*input_states, _id=manual_box_id, **ws_param_bindings)

  # TODO: remove type ignore after https://github.com/python/mypy/issues/2087 is resolved
  wrapper.has_sideeffect = bool(secs)  # type: ignore
  return wrapper


def ws_name(name: str):
  '''Specifies the name of the wrapped subworkspace.

  Example use::

    @ws_name('My nice workspace')
    @subworkspace
    def my_func(input1):
      return input1.sql1(sql='select * from vertices')

    my_func(lk.createExampleGraph())
  '''
  def decorator(fn: Callable):
    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
      return fn(*args, _ws_name=name, **kwargs)
    return wrapper
  return decorator


def ws_param(name: str, default: str = '', description: str = ''):
  '''Adds a workspace parameter to the wrapped subworkspace.

  Example use::

    @ws_param('p1')
    @ws_param('p2', default='x', description='The second column.')
    @subworkspace
    def my_func(input1):
      return input1.sql1(sql=pp('select $p1, $p2 from vertices'))

    my_func(lk.createExampleGraph(), p1='age', p2='name')

  Often you just want to pass your workspace parameter along unchanged. You could just write
  ``box(param=pp('$ws_param'))``. But if you take a keyword argument with the same name as a
  workspace parameter, this is even easier::

    @ws_params('p1')
    @subworkspace
    def my_func(input1, p1):
      return input1.sql1(sql=p1)  # Equivalent to sql=pp('$p1').
  '''
  def decorator(fn: Callable):
    @functools.wraps(fn)
    def wrapper(*args, _ws_params=[], **kwargs):
      return fn(*args, _ws_params=_ws_params + [text(name, default=default)], **kwargs)
    return wrapper
  return decorator


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


def _to_input_map(
        name: str, input_names: Collection[str], input_values: Collection) -> Dict[str, 'State']:
  '''Makes a dictionary from the input names and values with a bunch of asserts.'''
  assert len(input_names) == len(input_values), \
      f'{name!r} received {len(input_values)} inputs (expected {len(input_names)})'
  inputs = dict(zip(input_names, input_values))
  for k, v in inputs.items():
    assert isinstance(v, State), f'Input {k!r} of {name!r} is not a State: {v!r}'
  return inputs


class LynxKite:
  '''A connection to a LynxKite instance.

  Some LynxKite API methods take a connection argument which can be used to communicate with
  multiple LynxKite instances from the same session. If no arguments to the constructor are
  provided, then a connection is created using the following environment variables:
  ``LYNXKITE_ADDRESS``, ``LYNXKITE_USERNAME``, ``LYNXKITE_PASSWORD``,
  ``LYNXKITE_PUBLIC_SSL_CERT``, ``LYNXKITE_OAUTH_TOKEN``, ``LYNXKITE_SIGNED_TOKEN``.
  '''

  def __init__(self, username: str = None, password: str = None, address: str = None,
               certfile: str = None, oauth_token: str = None, signed_token: str = None,
               box_catalog: BoxCatalog = None) -> None:
    '''Creates a connection object.'''
    # Authentication and querying environment variables is deferred until the
    # first request.
    self._address = address
    self._username = username
    self._password = password
    self._certfile = certfile
    self._oauth_token = oauth_token
    self._signed_token = signed_token
    self._session = None
    self._pid = None
    self._operation_names: List[str] = []
    self._import_box_names: List[str] = [
        'importCSV', 'importJDBC', 'importJSON',
        'importORC', 'importParquet', 'importFromHive',
        'importNeo4j']
    self._export_box_names: List[str] = [
        'exportToCSV', 'exportToJSON', 'exportToParquet',
        'exportToJDBC', 'exportToORC', 'exportToHive']
    self._box_catalog = box_catalog  # TODO: create standard offline box catalog

  def home(self) -> str:
    return f'Users/{self.username()}/'

  def operation_names(self) -> List[str]:
    if not self._operation_names:
      box_names = self.box_catalog().box_names()
      self._operation_names = box_names + self.import_operation_names() + self.export_operation_names()
    return self._operation_names

  def import_operation_names(self) -> List[str]:
    '''When we use an import operation instead of an import box,
    "run import" will be triggered automatically.'''
    return [name + 'Now' for name in self._import_box_names]

  def export_operation_names(self) -> List[str]:
    '''When we use an export operation instead of an export box,
    the export will be triggered automatically. '''
    return [name + 'Now' for name in self._export_box_names]

  def box_catalog(self) -> BoxCatalog:
    if not self._box_catalog:
      bc = self._ask(
          '/ajax/boxCatalog',
          dict(ref=dict(top='', customBoxStack=[]))).boxes
      boxes = {}
      for box in bc:
        if box.categoryId != 'Custom boxes':
          boxes[_python_name(box.operationId)] = box
      self._box_catalog = BoxCatalog(boxes)
    return self._box_catalog

  def __dir__(self) -> Iterable[str]:
    return itertools.chain(super().__dir__(), self.operation_names())

  def __getattr__(self, name) -> Callable:

    def add_box_with_inputs(box_name, args, kwargs):
      inputs = _to_input_map(box_name, self.box_catalog().inputs(box_name), args)
      box = _new_box(self.box_catalog(), self, box_name, inputs=inputs, parameters=kwargs)
      return box

    def f(*args, **kwargs):
      if name in self.import_operation_names():
        real_name = name[:-len('Now')]
        box = add_box_with_inputs(real_name, args, kwargs)
        # If it is an import operation, we trigger the import here,
        # and return the modified (real) import box.
        box_json = box.to_json(
            id_resolver=lambda _: 'untriggered_import_box', workspace_root='', subworkspace_path='')
        import_result = self._send('/ajax/importBox', {'box': box_json})
        box.parameters['imported_table'] = import_result.guid
        box.parameters['last_settings'] = import_result.parameterSettings
      elif name in self.export_operation_names():
        # If it is an export operation, we trigger the export here.
        box = getattr(self, name[:-len('Now')])(*args, **kwargs)
        box.trigger()
      else:
        box = add_box_with_inputs(name, args, kwargs)
      return box

    if name.startswith('_'):  # To avoid infinite recursion in copy/deepcopy
      raise AttributeError()
    elif name not in self.operation_names():
      raise AttributeError('{} is not defined'.format(name))
    return f

  def sql(self, sql: str, *args, **kwargs) -> 'Box':
    '''Shorthand for sql1, sql2, ..., sql10 boxes.

    Use with positional arguments to go with the default naming, like::

      lk.sql('select name from one join two', state1, state2)

    Or pass the states in keyword arguments to use custom input names::

      lk.sql('select count(*) from people', people=state1)

    In either case you can pass in extra keyword arguments which will be passed on to the SQL boxes.
    '''
    input_states = list(args)
    input_names = []
    # Move named inputs from kwargs to input_states.
    for k, v in list(kwargs.items()):
      if isinstance(v, State):
        input_states.append(v)
        input_names.append(k)
        del kwargs[k]
    num_inputs = len(input_states)
    assert num_inputs > 0, 'SQL needs at least one input.'
    assert num_inputs < 11, 'SQL can have at most ten inputs.'
    box_name = 'sql{}'.format(num_inputs)
    inputs = _to_input_map(box_name, self.box_catalog().inputs(box_name), input_states)
    kwargs['sql'] = dedent(sql)
    # Use default names for unnamed inputs, custom names for the named ones.
    kwargs['input_names'] = ', '.join(self.box_catalog().inputs(box_name)[:len(args)] + input_names)
    return _new_box(self.box_catalog(), self, box_name, inputs=inputs, parameters=kwargs)

  def address(self) -> str:
    return self._address or os.environ['LYNXKITE_ADDRESS']

  def username(self) -> str:
    username = self._username or os.environ.get('LYNXKITE_USERNAME')
    if not username:
      signed_token = self.signed_token()
      assert signed_token, 'Can not determine username. Either set username or signed token.'
      return signed_token.split('|')[0]
    return username

  def password(self) -> Optional[str]:
    return self._password or os.environ.get('LYNXKITE_PASSWORD')

  def certfile(self) -> Optional[str]:
    return self._certfile or os.environ.get('LYNXKITE_PUBLIC_SSL_CERT')

  def oauth_token(self) -> Optional[str]:
    return self._oauth_token or os.environ.get('LYNXKITE_OAUTH_TOKEN')

  def signed_token(self) -> Optional[str]:
    return self._signed_token or os.environ.get('LYNXKITE_SIGNED_TOKEN')

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
    elif self.signed_token():
      r = self._request(
          '/signedUsernameLogin',
          dict(token=self.signed_token()))
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
    data = json.dumps(payload, default=_json_encode)
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
        params=dict(q=json.dumps(payload, default=_json_encode)),
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
    return self._send('/remote/removeName', dict(name=name, force=force))

  def change_acl(self, file: str, readACL: str, writeACL: str):
    '''Sets the read and write access control list for a path in LynxKite.'''
    return self._send('/remote/changeACL',
                      dict(graph=file, readACL=readACL, writeACL=writeACL))

  def list_dir(self, dir: str = '') -> List[types.SimpleNamespace]:
    '''List the objects in a directory, with their names, types, notes,
    and other optional data about graphs.'''

    return self._send('/remote/list', dict(path=dir)).entries

  def upload(self, data: bytes, name: str = None):
    '''Uploads a file that can then be used in import methods.

      prefixed_path = lk.upload('id,name\\n1,Bob')
    '''
    if name is None:
      name = 'remote-api-upload'  # A hash will be added anyway.
    return self._post('/ajax/upload', files=dict(file=(name, data))).text

  def uploadCSVNow(self, data: bytes, name: str = None):
    '''Uploads CSV data and returns a table state.'''
    filename = self.upload(data, name)
    return self.importCSVNow(filename=filename)

  def uploadParquetNow(self, data: bytes, name: str = None):
    '''Uploads Parquet file and returns a table state.'''
    filename = self.upload(data, name)
    return self.importParquetNow(filename=filename)

  def clean_file_system(self):
    """Deletes the data files which are not referenced anymore."""
    return self._send('/remote/cleanFileSystem')

  def get_data_files_status(self):
    '''Returns the amount of space used by LynxKite data, various cleaning methods
    and the amount of space they can free up.'''
    return self._ask('/ajax/getDataFilesStatus')

  def move_to_cleaner_trash(self, method: str):
    '''Moves LynxKite data files specified by the cleaning ``method`` into the cleaner trash.
    The possible values of ``method`` are defined in the result of get_data_files_status.'''
    return self._send('/ajax/moveToCleanerTrash', dict(method=method))

  def empty_cleaner_trash(self):
    '''Empties the cleaner trash.'''
    return self._send('/ajax/emptyCleanerTrash')

  def set_cleaner_min_age(self, days: float):
    return self._send('/ajax/setCleanerMinAge', dict(days=days))

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
    main_name = ws.safename()
    needed_custom_boxes = self.recursively_collect_customboxes(ws, main_name)
    needed_ws = {(path, box.workspace) for path, box in needed_custom_boxes}

    # Check name duplication in required workspaces
    names = [name for (name, rws) in needed_ws]
    if len(names) != len(set(names)):
      duplicates = [k for k, v in Counter(names).items() if v > 1]
      raise Exception(f'Duplicate custom box name(s): {duplicates}')
    # Check if the "main" ws name conflicts with one of the custom box names
    if main_name in names:
      raise Exception(f'Duplicate name: {main_name}')

    for name, rws in needed_ws:
      self.save_workspace(ws_root + '/' + name, _layout(rws.to_json(ws_root, name)))

    self.save_workspace(
        ws_root + '/' + main_name, _layout(ws.to_json(ws_root, main_name)))

    # We return the root directory and full name of the saved main workspace
    return (ws_root,
            _normalize_path(ws_root + '/' + main_name))

  def recursively_collect_customboxes(
          self, ws: 'Workspace', path) -> Set[Tuple[str, 'CustomBox']]:
    collected: Set[Tuple[str, CustomBox]] = set()
    for box in ws.custom_boxes():
      if box.workspace.name:
        box_path = box.workspace.name
      else:
        box_path = f'{path}_subs/{ws.id_of(box)}'
      if (box_path, box.workspace) in collected:
        continue
      collected.add((box_path, box))
      collected.update(self.recursively_collect_customboxes(box.workspace, box_path))
    return collected

  def fetch_workspace_output_states(self, ws: 'Workspace',
                                    save_under_root: str = None,
                                    ) -> Dict[Tuple[str, str], types.SimpleNamespace]:
    ws_root, _ = self.save_workspace_recursively(ws, save_under_root)
    return self.fetch_states(ws.to_json(ws_root, ws.safename()))
    # TODO: clean up saved workspaces if save_under_root is not set.

  def get_state_id(self, state: 'State') -> str:
    ws = Workspace(terminal_boxes=[state.box], name='Anonymous')
    workspace_outputs = self.fetch_workspace_output_states(ws)
    box_id = ws.id_of(state.box)
    plug = state.output_plug_name
    output = workspace_outputs[box_id, plug]
    _assert_lk_success(output, box_id, plug)
    return output.stateId

  def get_graph_attribute(self, guid: str) -> types.SimpleNamespace:
    return self._ask('/ajax/scalarValue', dict(scalarId=guid))

  def get_graph(self, state: str, path: str = '') -> types.SimpleNamespace:
    return self._ask('/ajax/getProjectOutput', dict(id=state, path=path))

  def get_export_result(self, state: str) -> types.SimpleNamespace:
    return self._ask('/ajax/getExportResultOutput', dict(stateId=state))

  def get_table_data(self, state: str, limit: int = -1) -> types.SimpleNamespace:
    return self._ask('/ajax/getTableOutput', dict(id=state, sampleRows=limit))

  def get_workspace(self, path: str, stack: List[str] = []) -> types.SimpleNamespace:
    return self._ask('/ajax/getWorkspace', dict(top=path, customBoxStack=stack))

  def get_workspace_boxes(self, path: str, stack: List[str] = []) -> List[types.SimpleNamespace]:
    return self.get_workspace(path, stack).workspace.boxes

  def import_box(self, boxes: List[SerializedBox], box_id: str) -> List[SerializedBox]:
    '''Equivalent to clicking the import button for an import box. Returns the updated boxes.'''
    boxes = to_simple_dicts(boxes)
    for box in boxes:
      if box['id'] == box_id:
        import_result = self._send('/ajax/importBox', {'box': box})
        box['parameters']['imported_table'] = import_result.guid
        box['parameters']['last_settings'] = import_result.parameterSettings
        return boxes
    raise KeyError(box_id)

  def export_box(self, outputs: Dict[Tuple[str, str], types.SimpleNamespace],
                 box_id: str) -> types.SimpleNamespace:
    '''Equivalent to triggering the export. Returns the exportResult output.'''
    output = outputs[box_id, 'exported']
    assert output.kind == 'exportResult', f'Output is {output.kind}, not "exportResult"'
    _assert_lk_success(output, box_id, 'exported')
    export = self.get_export_result(output.stateId)
    if export.result.computeProgress != 1:
      scalar = self.get_graph_attribute(export.result.id)
      assert scalar.string == 'Export done.', scalar.string
      export = self.get_export_result(output.stateId)
      assert export.result.computeProgress == 1, 'Failed to compute export result graph attribute.'
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
                 inputs: List[str] = None,
                 with_side_effects: bool = False
                 ) -> Callable[[Callable[..., Dict[str, 'State']]], 'Workspace']:
    se_collector = SideEffectCollector()

    def ws_decorator(builder_fn):
      real_name = builder_fn.__name__ if not name else name
      if inputs:
        names = inputs
      elif with_side_effects:
        names = list(inspect.signature(builder_fn).parameters.keys())[1:]
      else:
        names = inspect.signature(builder_fn).parameters.keys()
      input_boxes = [self.input(name=name) for name in names]
      if with_side_effects:
        results = builder_fn(se_collector, *input_boxes)
      else:
        results = builder_fn(*input_boxes)
      outputs = _to_outputs(results)
      return Workspace(name=real_name,
                       terminal_boxes=outputs + se_collector.top_level_side_effects,
                       side_effect_paths=list(se_collector.all_triggerables()),
                       input_boxes=input_boxes,
                       ws_parameters=parameters)
    return ws_decorator

  def workspace(self,
                name: str = None,
                parameters: List[WorkspaceParameter] = [],
                inputs: List[str] = None,
                ) -> Callable[[Callable[..., Dict[str, 'State']]], 'Workspace']:
    return self._workspace(name, parameters, inputs, with_side_effects=False)

  def workspace_with_side_effects(self,
                                  name: str = None,
                                  parameters: List[WorkspaceParameter] = [],
                                  inputs: List[str] = None,
                                  ) -> Callable[[Callable[..., Dict[str, 'State']]], 'Workspace']:
    return self._workspace(name, parameters, inputs, with_side_effects=True)

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

  def from_df(self, df, index=True):
    """Converts a pandas dataframe to a LynxKite table.

    Set `index` to False if you don't want to include the index.
    Warning: the `index` setting feature only works with pandas>=0.24.0
    """
    import pandas
    pandas_version = pandas.__version__
    major, minor, _ = map(int, pandas_version.split('.'))
    false_index_not_supported = major == 0 and minor < 24
    if false_index_not_supported:
      kwargs = {}
      if index == False:
        import warnings
        from textwrap import dedent
        msg = dedent(f'''\
          The setting index=False in from_df function is only supported with pandas>=0.24.0.
          Your pandas version is {pandas_version} so the index parameter is reset to True.''')
        warnings.warn(msg)
    else:
      kwargs = {'index': index}
    with NamedTemporaryFile() as f:
      df.to_parquet(f.name, **kwargs)
      return self.uploadParquetNow(f.read())

  def set_executors(self, count):
    return self._send('/remote/setExecutors', {'count': count})


class State:
  '''Represents a named output plug of a box.

  It can recursively store the boxes which are connected to the input plugs of
  the box of this state.
  '''

  def __init__(self, box: 'Box', output_plug_name: str) -> None:
    self.output_plug_name = output_plug_name
    self.box = box

  def operation_names(self):
    return self.box.bc.box_names() + self.box.lk.export_operation_names()

  def __getattr__(self, name: str) -> Callable:

    def f(**kwargs):
      if name in self.box.lk.export_operation_names():
        export_box = getattr(self, name[:-len('Now')])(**kwargs)
        export_box.trigger()
        return export_box
      else:
        inputs = self.box.bc.inputs(name)
        # This chaining syntax is only allowed for boxes with exactly one input.
        assert len(inputs) > 0, '{} does not have an input'.format(name)
        assert len(inputs) < 2, '{} has more than one input'.format(name)
        [input_name] = inputs
        return _new_box(
            self.box.bc,
            self.box.lk,
            name,
            inputs={input_name: self},
            parameters=kwargs)

    if name.startswith('_'):  # To avoid infinite recursion in copy/deepcopy
      raise AttributeError()
    elif name not in self.operation_names():
      raise AttributeError('{} is not defined on {}'.format(name, self))
    return f

  def __dir__(self) -> Iterable[str]:
    return itertools.chain(super().__dir__(), self.operation_names())

  def __str__(self) -> str:
    return "Output {} of box {}".format(self.output_plug_name, self.box)

  def sql(self, sql: str, **kwargs) -> 'SingleOutputAtomicBox':
    return self.sql1(sql=dedent(sql), **kwargs)

  def persist(self) -> 'SingleOutputAtomicBox':
    '''Same as ``x.sql('select * from input', persist='yes')``.'''
    return self.sql('select * from input', persist='yes')

  def df(self, limit: int = -1):
    '''Returns a Pandas DataFrame if this state is a table.'''
    import pandas

    def get(v, t):
      if not v.defined:
        return None
      if hasattr(v, 'double'):
        return v.double
      elif t == 'java.math.BigDecimal':
        return float(v.string)
      else:
        return v.string

    table = self.get_table_data(limit)
    header = [c.name for c in table.header]
    types = [c.dataType for c in table.header]
    data = [[get(c, t) for (c, t) in zip(r, types)] for r in table.data]
    return pandas.DataFrame(data, columns=header)

  def columns(self):
    '''Returns a list of columns if this state is a table.'''
    return list(self.df(0).columns)

  def get_table_data(self, limit: int = -1) -> types.SimpleNamespace:
    '''Returns the "raw" table data if this state is a table.'''
    return self.box.lk.get_table_data(self.box.lk.get_state_id(self), limit)

  def get_graph(self) -> types.SimpleNamespace:
    '''Returns the graph metadata if this state is a graph.'''
    return self.box.lk.get_graph(self.box.lk.get_state_id(self))

  def get_progress(self):
    '''Returns progress info about the state.'''
    lk = self.box.lk
    state_id = lk.get_state_id(self)
    progress = lk._ask('/ajax/long-poll', dict(syncedUntil=0, stateIds=[state_id]))
    return progress.progress.__dict__[state_id]

  def run_export(self) -> str:
    '''Triggers the export if this state is an ``exportResult``.

    Returns the prefixed path of the exported file. This method is deprecated,
    only used in tests, where we need the export path.
    '''
    lk = self.box.lk
    state_id = lk.get_state_id(self)
    export = lk.get_export_result(state_id)
    if export.result.computeProgress != 1:
      scalar = lk.get_graph_attribute(export.result.id)
      assert scalar.string == 'Export done.', scalar.string
      export = lk.get_export_result(state_id)
      assert export.result.computeProgress == 1, 'Failed to compute export result graph attribute.'
    return export.parameters.path

  def compute(self) -> None:
    '''Triggers the computation of this state.

    Uses a temporary folder to save a temporary workspace for this computation.
    '''
    self.computeInputs().trigger()

  def save_snapshot(self, path: str) -> None:
    '''Save this state as a snapshot under path.'''
    lk = self.box.lk
    state_id = lk.get_state_id(self)
    lk.save_snapshot(path, state_id)

  def save_to_sequence(self, tss, date: datetime.datetime) -> None:
    '''Save this state to the ``tss`` TableSnapshotSequence with ``date`` as
    the date of the snapshot.'''
    lk = self.box.lk
    state_id = lk.get_state_id(self)
    tss.save_to_sequence(state_id, date)


class Placeholder:
  '''Universal placeholder. Use it whenever you need to hold a place.'''

  def __init__(self, value=None) -> None:
    self.value = value


def _fn_id(fn: Callable):
  '''Creates a string id for a function which only depends on the fully qualified name of the callable.
  This can be used as a replacement for BoxPath (unique box identity) in case of external boxes.
  '''
  return f'{fn.__module__}_{fn.__qualname__}'.replace('.', '_')


def _is_lambda(f: Callable):
  def LAMBDA(): return 0
  return isinstance(f, type(LAMBDA)) and f.__name__ == LAMBDA.__name__


def _is_instance_method(f: Callable):
  return inspect.ismethod(f)


def external(fn: Callable):
  '''Decorator for executing computation outside of LynxKite in a LynxKite workflow.

  Returns a custom box that internally exports the input tables and runs the external computation on
  them when it is triggered. The output of this custom box is the result of the external
  computation as a LynxKite table.

  Example::

    @external
    def titled_names(table, default):
      df = table.pandas()
      df['gender'] = df.gender.fillna(default)
      df['titled'] = np.where(df.gender == 'Female', 'Ms ' + df.name, 'Mr ' + df.name)
      return df

    t = titled_names(lk.createExampleGraph(), 'Female')
    t.trigger() # Causes the function to run.
    print(t.df())

  Table state parameters of the function call are exported into a Parquet file. You can read them
  manually, or via the methods of the ``InputTable`` objects that are passed to your function in
  their place:

  - ``filename`` is the Hadoop path for the file. (I.e. ``file:/something`` or
    ``hdfs:///something``.)
  - ``pandas()`` loads the Parquet file into a Pandas DataFrame.
  - ``spark()`` loads the Parquet file into a PySpark DataFrame.
  - ``lk()`` loads the Parquet file into a LynxKite state.

  Your function can return one of the following types:

  - ``pandas.DataFrame`` to be written to a Parquet file and imported in LynxKite.
  - ``spark.DataFrame`` to be written to a Parquet file and imported in LynxKite.
  - A string that is the LynxKite prefixed path to a Parquet file that is your output.
  - A LynxKite table state to be written to a Parquet file and imported in LynxKite.

  Why use LynxKite states in external computations? This allows the use of LynxKite as in a notebook
  environment. You can access the actual data and write code that is conditional on the data.
  '''

  assert not _is_lambda(fn), 'You cannot use lambda functions for external computation.'
  assert not _is_instance_method(fn), 'You cannot use instance methods for external computation.'

  @subworkspace
  @functools.wraps(fn)
  def wrapper(*args, sec=SideEffectCollector.AUTO, **kwargs):
    exports = []

    def add_placeholder(name, value):
      if isinstance(value, State):
        exports.append(value.exportToParquet(for_download="yes"))
        return Placeholder(len(exports) - 1)  # The corresponding input index.
      else:
        return value

    signature = inspect.signature(fn, follow_wrapped=False)
    bound = signature.bind(*args, **kwargs)
    bound = map_args(signature, bound, add_placeholder)
    assert exports, 'Please pass in at least one LynxKite state input.'
    lk = exports[0].box.lk
    external = ExternalComputationBox(
        lk.box_catalog(), lk, exports,
        {'snapshot_prefix': f'tmp_workspaces/tmp_snapshots/external-{_fn_id(fn)}-'},
        fn, bound)

    for export in exports:
      export.register(sec)
    external.register(sec)
    return external
  return wrapper


class _DownloadableLKTable:
  def __init__(self, lk, lk_path, tmp_file):
    self.lk = lk
    self.lk_path = lk_path
    self.tmp_file = tmp_file
    self._is_downloaded = False

  def download(self):
    if not self._is_downloaded:
      self._download()
    return self.tmp_file.name

  def _download(self):
    data = bytes(self.lk.download_file(self.lk_path))
    self.tmp_file.write(data)
    self.tmp_file.flush()

  def close(self):
    self.tmp_file.close()


class _LKTableContext:
  '''A context manager for downloading LK tables to temporary local files.

  Example usage:

     with _LKTableContext(lk) as ctx:
       t = ctx.table(lk_path)
       local_path = t.download()
       # Do some stuff with the local file while it is available.
       ...
     # After exiting the context the local file is closed and deleted.
  '''

  def __init__(self, lk):
    self.lk = lk
    self.lk_tables = []

  def table(self, lk_path):
    f = NamedTemporaryFile()
    table = _DownloadableLKTable(self.lk, lk_path, f)
    self.lk_tables.append(table)
    return table

  def __enter__(self):
    return self

  def __exit__(self, type, value, traceback):
    for t in self.lk_tables:
      t.close()


class InputTable:
  '''Input tables for external computations (``@external``) are translated to these objects.'''

  def __init__(self, lk, lk_table: _DownloadableLKTable) -> None:
    self._lk = lk
    self.lk_table = lk_table

  def pandas(self):
    '''Returns a Pandas DataFrame.'''
    import pandas
    downloaded = self.lk_table.download()
    return pandas.read_parquet(downloaded)

  def spark(self, spark):
    '''Takes a SparkSession as the argument and returns the table as a Spark DataFrame.'''
    downloaded = self.lk_table.download()
    return spark.read.parquet("file://" + downloaded)

  def lk(self) -> State:
    '''Returns a LynxKite State.'''
    return self._lk.importParquetNow(filename=self.lk_table.lk_path)


def _is_spark_dataframe(x):
  try:
    from pyspark.sql.dataframe import DataFrame
  except ImportError:
    return False  # It cannot be a Spark DataFrame if we don't even have Spark.
  return isinstance(x, DataFrame)


def _is_pandas_dataframe(x):
  try:
    import pandas as pd
  except ImportError:
    return False  # It cannot be a Pandas DataFrame if we don't even have Pandas.
  return isinstance(x, pd.DataFrame)


class Box:
  '''Represents a box in a workspace segment.

  It can store workspace segments, connected to its input plugs.
  '''

  def __init__(self, box_catalog: BoxCatalog, lk: LynxKite,
               inputs: Dict[str, State], parameters: Dict[str, Any],
               manual_box_id: str = None) -> None:
    self.bc = box_catalog
    self.lk = lk
    self.inputs = inputs
    self.all_parameters = parameters
    self.parameters: Dict[str, str] = {}
    self.parametric_parameters: Dict[str, str] = {}
    self.outputs: Set[str] = set()
    self.manual_box_id = manual_box_id
    # We separate normal and parametric parameters here.
    # Parametric parameters can be specified as `name=PP('parametric value')`
    for key, value in parameters.items():
      if isinstance(value, ParametricParameter):
        self.parametric_parameters[key] = str(value)
      else:
        self.parameters[key] = str(value)

  def _operation_id(self, workspace_root: str, subworkspace_path: str) -> str:
    '''The id that we send to the backend to identify a box.'''
    raise NotImplementedError()

  def box_id_base(self) -> str:
    '''The base of the box_id, which is used when we save a workspace,
    containing this box.'''
    raise NotImplementedError()

  def name(self) -> str:
    '''Either the name in the box catalog or the name under which the box is saved.'''
    raise NotImplementedError()

  def to_json(
          self, id_resolver: Callable[['Box'], str], workspace_root: str,
          subworkspace_path: str) -> SerializedBox:
    '''Creates the json representation of a box in a workspace.

    The inputs have to be connected, and all the attributes have to be
    defined when we call this.
    '''
    def input_state(state):
      return {'boxId': id_resolver(state.box), 'id': state.output_plug_name}

    operation_id = self._operation_id(
        workspace_root, subworkspace_path + '_subs/' + id_resolver(self))
    return SerializedBox({
        'id': id_resolver(self),
        'operationId': operation_id,
        'parameters': self.parameters,
        'x': 0, 'y': 0,
        'inputs': {plug: input_state(state) for plug, state in self.inputs.items()},
        'parametricParameters': self.parametric_parameters})

  def register(self, side_effect_collector: 'SideEffectCollector'):
    side_effect_collector.add_box(self)
    return self

  def __getitem__(self, index: str) -> State:
    if index not in self.outputs:
      raise KeyError(index)
    return State(self, index)

  def trigger(self) -> None:
    '''Triggers this box.

    Can be used on triggerable boxes like `saveToSnapshot` and export boxes.
    '''
    sec = SideEffectCollector()
    self.register(sec)
    ws = Workspace(
        terminal_boxes=[self],
        side_effect_paths=list(sec.all_triggerables()),
        name='Anonymous')
    ws.trigger_all_side_effects()

  def _trigger_in_ws(self, ws: str, box: str, stack: List[str]) -> None:
    '''Used for triggering this box anywhere in a saved workspace.'''
    self.lk.trigger_box(ws, box, stack)

  def is_box(self, operation: str) -> bool:
    '''Checks if the box is the `operation` box.'''
    return isinstance(self, AtomicBox) and self.operation == operation


class AtomicBox(Box):
  '''
  An ``AtomicBox`` is a ``Box`` that can not be further decomposed. It corresponds to a single
  frontend operation.
  '''

  def __init__(self, box_catalog: BoxCatalog, lk: LynxKite, operation: str,
               inputs: Dict[str, State], parameters: Dict[str, Any],
               manual_box_id: str = None) -> None:
    super().__init__(box_catalog, lk, inputs, parameters, manual_box_id)
    self.operation = operation
    self.outputs = set(self.bc.outputs(operation))
    exp_inputs = set(self.bc.inputs(operation))
    got_inputs = inputs.keys()
    assert got_inputs == exp_inputs, 'Got box inputs: {}. Expected: {}'.format(
        got_inputs, exp_inputs)

  def _operation_id(self, workspace_root, subworkspace_path):
    return self.bc.operation_id(self.operation)

  def box_id_base(self) -> str:
    return self.operation

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
               inputs: Dict[str, State], parameters: Dict[str, Any], output_name: str,
               manual_box_id: str = None) -> None:
    AtomicBox.__init__(self, box_catalog, lk, operation, inputs, parameters, manual_box_id)
    State.__init__(self, self, output_name)


class DataFrameSender:
  '''Class to send some dataframe to LynxKite in Parquet format.
    We do this by saving the dataframe to a local Parquet file
    and then upload it to LynxKite.
  '''

  def __init__(self, lk):
    self.lk = lk

  def save_dataframe_to_local_file(self, df, tmp_dir) -> str:
    '''Saves the dataframe as a single Parquet file in tmpdir
    Returns the actual path of the binary that was written.
    '''
    raise NotImplementedError('Must be implemented in the subclass')

  def send(self, df):
    '''Sends the local Parquet file to LynxKite'''
    with TemporaryDirectory() as tmp_dir:
      tmp_path = tmp_dir + '/parquet'
      parquet_file = self.save_dataframe_to_local_file(df, tmp_path)
      with open(parquet_file, "rb") as fin:
        return self.lk.uploadParquetNow(fin.read())


class PandasDataFrameSender(DataFrameSender):

  def save_dataframe_to_local_file(self, df, path) -> str:
    df.to_parquet(path)
    return path


class SparkDataFrameSender(DataFrameSender):

  def save_dataframe_to_local_file(self, df, target_dir) -> str:
    p = df.rdd.getNumPartitions()
    if p != 1:
      df = df.repartition(1)
    df.write.parquet(target_dir)
    parquet_files = [f for f in os.listdir(target_dir) if f.startswith('part-')]
    assert len(parquet_files) == 1, f'Only one parquet file is expected here: {parquet_files}'
    return target_dir + '/' + parquet_files[0]


class ExternalComputationBox(SingleOutputAtomicBox):
  '''
  A box that runs external computation when triggered. Use it via ``@external``.
  '''

  def __init__(self, box_catalog: BoxCatalog, lk: LynxKite,
               inputs: List[State], parameters: Dict[str, Any],
               fn: Callable, args: inspect.BoundArguments, manual_box_id: str = None) -> None:
    SingleOutputAtomicBox.__init__(
        self, box_catalog, lk, f'externalComputation{len(inputs)}',
        {str(i + 1): inputs[i] for i in range(len(inputs))}, parameters, 'table', manual_box_id)
    self.fn = fn
    self.args = args

  def _trigger_in_ws(self, wsname: str, box: str, stack: List[str]) -> None:
    lk = self.lk

    tmpfile_list: List[str] = []

    with _LKTableContext(lk) as ctx:
      # Find inputs.
      resp = lk.get_workspace(wsname, stack)
      boxes = {b.id: b for b in resp.workspace.boxes}
      input_tables = [getattr(boxes[box].inputs, str(i + 1)) for i in range(len(self.inputs))]
      states = {(o.boxOutput.boxId, o.boxOutput.id): o.stateId for o in resp.outputs}
      input_states = [states[t.boxId, t.id] for t in input_tables]
      export_results = [lk.get_export_result(s) for s in input_states]
      snapshot_prefix = self.parameters['snapshot_prefix']
      snapshot_guids = '-'.join(exp.result.id for exp in export_results)

      def get_input_table(name, value):
        if isinstance(value, Placeholder):
          path = export_results[value.value].parameters.path
          return InputTable(lk, ctx.table(path))
        else:
          return value

      signature = inspect.signature(self.fn, follow_wrapped=False)
      bound = map_args(signature, self.args, get_input_table)
      # Run external function.
      res = self.fn(*bound.args, **bound.kwargs)
      # Import results.
      if _is_spark_dataframe(res):
        state = SparkDataFrameSender(self.lk).send(res)
      elif _is_pandas_dataframe(res):
        state = PandasDataFrameSender(self.lk).send(res)
      elif isinstance(res, State):
        state = res
      elif isinstance(res, str):
        assert '$' in res, f'The output path has must be a LynxKite prefixed path. Got: {res!r}'
        state = lk.importParquetNow(filename=res)
        # TODO: Delete imported file.
      else:
        raise Exception(
            f'The return value from {self.fn.__name__}() is not a supported object. Got: {res!r}')

      state.save_snapshot(snapshot_prefix + snapshot_guids)


class CustomBox(Box):
  '''
  A ``CustomBox`` is a ``Box`` composed of multiple other boxes.
  '''

  def __init__(self, box_catalog: BoxCatalog, lk: LynxKite, workspace: 'Workspace',
               inputs: Dict[str, State], parameters: Dict[str, Any],
               manual_box_id: str = None) -> None:
    super().__init__(box_catalog, lk, inputs, parameters, manual_box_id)
    self.workspace = workspace
    self.outputs = set(workspace.outputs)
    exp_inputs = set(workspace.inputs)
    got_inputs = inputs.keys()
    assert got_inputs == exp_inputs, 'Got box inputs: {}. Expected: {}'.format(
        got_inputs, exp_inputs)

  def _operation_id(self, workspace_root: str, subworkspace_path: str):
    if self.workspace.name:
      return _normalize_path(workspace_root + '/' + self.workspace.name)
    else:
      return _normalize_path(workspace_root + '/' + subworkspace_path)

  def box_id_base(self):
    return self.workspace.custom_box_id_base

  def name(self):
    return self.workspace.name

  def __str__(self) -> str:
    return "Custom box {} with parameters {} and inputs {}".format(
        self.name(),
        self.parameters,
        self.inputs)

  def snatch(self, box: Box) -> Box:
    """Takes a box that is inside the workspace referred to by this custom box and
    returns an equivalent box that is accessible from outside.
    """
    new_terminal_boxes = [box[o].output(name=o) for o in box.outputs]
    new_ws = Workspace(
        terminal_boxes=new_terminal_boxes,
        name=self.workspace.name,
        custom_box_id_base=self.workspace.custom_box_id_base,
        side_effect_paths=self.workspace._side_effect_paths,
        input_boxes=self.workspace.input_boxes,
        ws_parameters=self.workspace._ws_parameters)
    input_list = [self.inputs[i] for i in self.workspace.inputs]
    return new_ws(*input_list, **self.all_parameters)


class SingleOutputCustomBox(CustomBox, State):
  '''
  An ``CustomBox`` with a single output. This makes chaining multiple operations after each other
  possible.
  '''

  def __init__(self, box_catalog: BoxCatalog, lk: LynxKite, workspace: 'Workspace',
               inputs: Dict[str, State], parameters: Dict[str, Any], output_name: str,
               manual_box_id: str = None) -> None:
    CustomBox.__init__(self, box_catalog, lk, workspace, inputs, parameters, manual_box_id)
    State.__init__(self, self, output_name)


def _python_name(name: str) -> str:
  '''Transforms a space separated string into a camelCase format.

  The operation "Use base graph as segmentation" will be called as
  ``useBaseGraphAsSegmentation``. Dashes are ommitted.
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
  manual_box_id = parameters.pop('_id', None)
  if isinstance(operation, str):
    outputs = bc.outputs(operation)
    if len(outputs) == 1:
      return SingleOutputAtomicBox(bc, lk, operation, inputs, parameters, outputs[0], manual_box_id)
    else:
      return AtomicBox(bc, lk, operation, inputs, parameters, manual_box_id)
  else:
    outputs = operation.outputs
    if len(outputs) == 1:
      return SingleOutputCustomBox(bc, lk, operation, inputs, parameters, outputs[0], manual_box_id)
    else:
      return CustomBox(bc, lk, operation, inputs, parameters, manual_box_id)


def _reverse_bfs_on_boxes(roots: List[Box], list_roots: bool = True) -> Iterator[Box]:
  '''
  Lists all the dependencies of boxes in ``roots`` in a bfs way.
  '''
  to_process = deque(roots)
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
    output_box = [box for box in source_box.workspace.output_boxes
                  if box.parameters['name'] == state.output_plug_name][0]
    return BoxPath(output_box, box_list + [source_box])


class BoxPath:
  '''Represents a box (which can be inside (nested) custom boxes).
  It can be used for example to trigger boxes inside custom boxes.

  ``stack[i+1]`` is always a box contained by the workspace referred by the
  custom box ``stack[i]`` and  ``base`` is a box contained by ``stack[-1]``.
  '''

  def __init__(self, base: Box, stack: List[CustomBox] = []) -> None:
    self.base = base
    self.stack = stack

  def __str__(self) -> str:
    workspaces = [cb.workspace for cb in self.stack]
    if self.stack:
      first = self.stack[0].box_id_base() + '_?'  # Don't know the id without the containing ws.
      rest = [ws.id_of(box) for ws, box in zip(workspaces, self.stack_and_base()[1:])]
      return '/'.join([first] + rest)
    else:
      return self.base.box_id_base() + '_?'

  def stack_and_base(self) -> List[Box]:
    # Create a new, generic list that we can append the AtomicBox to.
    stack: List[Box] = list(self.stack)
    return stack + [self.base]

  def to_string_id(self, outer_ws) -> str:
    '''Can be used in automation, to generate unique task ids.

    stack[0] is supposed to be in the outer_ws workspace.
    '''
    workspaces = [outer_ws] + [cb.workspace for cb in self.stack]
    return '/'.join(ws.id_of(box) for ws, box in zip(workspaces, self.stack_and_base()))

  def add_box_as_prefix(self, box: CustomBox) -> 'BoxPath':
    return BoxPath(self.base, [box] + self.stack)

  def add_box_as_base(self, new_base: Box) -> 'BoxPath':
    """Takes a box inside the current base as the new base and puts the current base
    on the top of the stack.
    """
    assert isinstance(self.base, CustomBox), 'Can only dive into a custom box.'
    assert new_base in self.base.workspace.all_boxes, f'{new_base} is not a box in {self.base}.'
    return BoxPath(new_base, self.stack + [self.base])

  def snatch(self) -> Box:
    """Returns a box that is accessible from outside and whose output is the same as the that of the
    box referred by this BoxPath.
    """
    last_box = self.base
    for box in reversed(self.stack):
      last_box = box.snatch(last_box)
    return last_box

  def parent(self, input_name: str) -> 'BoxPath':
    parent_state = self.base.inputs[input_name]
    return _atomic_source_of_state(self.stack, parent_state)

  def parents(self) -> List['BoxPath']:
    box = self.base
    if box.inputs:  # normal box with inputs
      return [self.parent(inp) for inp in box.inputs.keys()]
    elif box.is_box('input') and self.stack:  # input box
      containing_custom_box = self.stack[-1]
      input_name = box.parameters['name']
      source_state = containing_custom_box.inputs[input_name]
      return [_atomic_source_of_state(self.stack[:-1], source_state)]
    else:  # no parents
      return []

  def dependency_representative(self) -> 'BoxPath':
    '''Returns the path of the box that should be used in dependency calculations.

    For most boxes (like SQL1) this is themselves. Some boxes (like Compute inputs) have no outputs
    or rarely have boxes consuming their outputs. For these boxes we use their inputs for the
    purposes of dependency calculations. A full example::

      [Create example graph] -> [Compute inputs]
        |
        v
      [SQL1]                 -> [Compute inputs]

    Only the Compute boxes are triggerable here, but there is no explicit dependency between them.
    You could trigger the second Compute box even if the first Compute box did not exist. But we
    want to order the Compute boxes in the intuitive way: as if the second one depended on the
    first. So we use their inputs as their representatives in the dependency calculation.
    '''
    box = self.base
    if any([box.is_box('output'),
            box.is_box('input') and self.stack,
            box.is_box('computeInputs'),
            box.is_box('saveToSnapshot'),
            isinstance(box, AtomicBox) and box.operation in box.lk._export_box_names]):
      parents = self.parents()
      assert len(parents) == 1, f'Cannot follow parent chain for {box}'
      return parents[0].dependency_representative()
    else:
      return self

  def to_dict(self):
    '''Returns a (human readable) dict representation of this object.'''
    parent = None
    base = self.base
    op = base.operation if isinstance(base, AtomicBox) else base.box_id_base()
    op_param = self.base.parameters
    if self.stack:
      parent = self.stack[-1].name()
    return dict(operation=op, params=op_param, nested_in=parent)

  def __hash__(self):
    return hash(','.join([str(box) for box in self.stack] + [str(self.base)]))

  def __eq__(self, other):
    return self.base == other.base and self.stack == other.stack

  @staticmethod
  def dependencies(bps: Collection['BoxPath']) -> Dict['BoxPath', Set['BoxPath']]:
    '''Returns the dependencies between the given boxes.'''
    bp_to_rep = {bp: bp.dependency_representative() for bp in bps}
    rep_to_bps: Dict[BoxPath, Set[BoxPath]] = defaultdict(set)
    dag: Dict[BoxPath, Set[BoxPath]] = {bp: set() for bp in bps}
    for bp, rep in bp_to_rep.items():
      rep_to_bps[rep].add(bp)
    for bp in bps:
      # Find dependencies of "bp" by searching the nodes upstream from its representative for
      # representatives of other boxes in "bps".
      rep = bp_to_rep[bp]
      if bp is not rep and rep in bps:
        assert bp_to_rep[rep] == rep, \
            f'If {rep} is the representative of {bp} it must also be its own representative.'
        dag[bp].add(rep)  # Our representative is in "bps". Depend on it.
      to_process = deque(rep.parents())
      visited: Set[BoxPath] = set()
      while to_process:
        box_path = to_process.pop()
        visited.add(box_path)
        if box_path in rep_to_bps:
          dag[bp].update(rep_to_bps[box_path])
        to_process.extend(p for p in box_path.parents() if p not in visited)
    return dag


class SideEffectCollector:

  AUTO: 'SideEffectCollector'  # For @subworkspace.

  def __init__(self):
    self.top_level_side_effects: List[Box] = []

  def add_box(self, box: Box) -> None:
    self.top_level_side_effects.append(box)

  def all_triggerables(self) -> Iterable[BoxPath]:
    for box in self.top_level_side_effects:
      yield from self._all_triggerables_in_box(box)

  def compute(self, state, id_override=None):
    self.add_box(state.computeInputs(_id=id_override))
    return state

  @staticmethod
  def _all_triggerables_in_box(box):
    if isinstance(box, AtomicBox):
      yield BoxPath(box)
    elif isinstance(box, CustomBox):
      for triggerable in box.workspace.side_effect_paths():
        yield triggerable.add_box_as_prefix(box)

  def __str__(self):
    btb = 'To build ==> ' + str([b.name() for b in self.top_level_side_effects])
    btt = ' To trigger ==> ' + str([btt for btt in self.all_triggerables()])
    return btb + btt


SideEffectCollector.AUTO = SideEffectCollector()


class Workspace:
  '''Immutable class representing a LynxKite workspace.'''

  def __init__(self,
               terminal_boxes: List[Box] = [],
               name: Optional[str] = None,
               custom_box_id_base: Optional[str] = None,
               side_effect_paths: List[BoxPath] = [],
               input_boxes: List[AtomicBox] = [],
               ws_parameters: List[WorkspaceParameter] = []) -> None:
    self.name = name
    assert any([name, custom_box_id_base]), 'Either "name" or "custom_box_id_base" has to be set.'
    self.custom_box_id_base = custom_box_id_base or name
    assert terminal_boxes, 'A workspace must contain at least one box'
    self.lk = terminal_boxes[0].lk
    self.all_boxes: Set[Box] = set()
    self.input_boxes = input_boxes
    self._box_ids: Dict[Box, str] = dict()
    self._next_ids: Dict[str, int] = defaultdict(int)  # Zero, by default.
    assert all(b.is_box('input') for b in input_boxes), 'Non-input box in input_boxes'
    self.inputs = [inp.parameters['name'] for inp in input_boxes]
    self.output_boxes = [box for box in terminal_boxes if box.is_box('output')]
    self.outputs = [outp.parameters['name'] for outp in self.output_boxes]
    self._ws_parameters = ws_parameters
    self._side_effect_paths = side_effect_paths
    self._terminal_boxes = terminal_boxes
    self._bc = self._terminal_boxes[0].bc
    for box in _reverse_bfs_on_boxes(self._terminal_boxes):
      self._add_box(box)
    # Check uniqueness of box ids
    box_ids = list(self._box_ids.values())
    if len(box_ids) != len(set(box_ids)):
      duplicates = [k for k, v in Counter(box_ids).items() if v > 1]
      raise Exception(f'Duplicate box id(s): {duplicates}')

  def safename(self) -> str:
    return self.name or 'Anonymous'

  def _add_box(self, box):
    self.all_boxes.add(box)
    if box.manual_box_id:
      self._box_ids[box] = box.manual_box_id
    else:
      base = box.box_id_base()
      index = self._next_ids[base]
      self._box_ids[box] = f'{base}_{index}'
      self._next_ids[base] += 1

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

  def to_json(self, workspace_root: str, subworkspace_path: str) -> List[SerializedBox]:
    non_anchor_boxes = [
        box.to_json(self.id_of, workspace_root, subworkspace_path) for box in self.all_boxes]
    # We use ws_parameters to customize _anchor_box.
    ab = copy.deepcopy(_anchor_box)
    ab['parameters'] = dict(parameters=self._ws_parameters_to_str())
    return [ab] + non_anchor_boxes

  def __str__(self):
    return json.dumps(self.to_json('', 'main'), indent=2)

  def custom_boxes(self) -> List[CustomBox]:
    return [
        box for box in self.all_boxes
        if isinstance(box, CustomBox)]

  def find(self, box_id_base: str) -> BoxPath:
    """Returns the BoxPath for the box nested in the workspace whose box_id_base
    is the given string.

    Raises an error if there is not exactly one such a box.
    """
    found = self.find_all(box_id_base)
    assert len(found) > 0, f'Found no box with box_id_base: {box_id_base}.'
    assert len(found) < 2, f'Found more than one box with box_id_base: {box_id_base}.'
    return found[0]

  def find_all(self, box_id_base: str) -> List[BoxPath]:
    """Returns the BoxPaths for all boxes nested in the workspace whose
    box_id_base is the given string.
    """
    found: List[BoxPath] = []
    for box in self.all_boxes:
      found.extend(self._find_all(box_id_base, BoxPath(box)))
    return found

  def _find_all(self, box_id_base: str, current_boxpath: BoxPath) -> List[BoxPath]:

    def good_box(box):
      if box_id_base == 'sql':
        return box.box_id_base() in [f'sql{i+1}' for i in range(10)]
      else:
        return box.box_id_base() == box_id_base

    found = []
    current_base = current_boxpath.base
    if good_box(current_base):
      found.append(current_boxpath)
    if isinstance(current_base, CustomBox):
      for box in current_base.workspace.all_boxes:
        box_path = current_boxpath.add_box_as_base(box)
        found.extend(self._find_all(box_id_base, box_path))
    return found

  def side_effect_paths(self) -> List[BoxPath]:
    return self._side_effect_paths

  def has_date_parameter(self) -> bool:
    return 'date' in [p.name for p in self._ws_parameters]

  def has_local_date_parameter(self) -> bool:
    return 'local_date' in [p.name for p in self._ws_parameters]

  def terminal_box_ids(self) -> List[str]:
    return [self.id_of(box) for box in self._terminal_boxes]

  def __call__(self, *args, **kwargs) -> Box:
    inputs = _to_input_map(self.safename(), self.inputs, args)
    return _new_box(self._bc, self.lk, self, inputs=inputs, parameters=kwargs)

  def _trigger_box(self, box_to_trigger: BoxPath, full_path: str):
    box_ids = self._box_to_trigger_to_box_ids(box_to_trigger)
    # The last id is a "normal" box id, the rest are the custom box stack.
    box_to_trigger.base._trigger_in_ws(full_path, box_ids[-1], box_ids[:-1])

  def save(self, saved_under_folder: str) -> str:
    lk = self.lk
    _, full_path = lk.save_workspace_recursively(self, saved_under_folder)
    return full_path

  def trigger_saved(self, box_to_trigger: BoxPath, saved_under_folder: str):
    ''' Triggers one side effect.

    Assumes the workspace is saved under `saved_under_root`.
    '''
    full_path = _normalize_path(saved_under_folder + '/' + self.safename())
    self._trigger_box(box_to_trigger, full_path)

  def trigger(self, box_to_trigger: BoxPath):
    ''' Triggers one side effect.

    Assumes the workspace is not saved, so saves it under a random folder.
    '''
    random_folder = _random_ws_folder()
    full_path = self.save(random_folder)
    self._trigger_box(box_to_trigger, full_path)

  def trigger_all_side_effects(self):
    ''' Triggers all side effects.

    Also saves the workspace under a temporary folder.
    '''
    temporary_folder = _random_ws_folder()
    self.save(temporary_folder)
    for btt in serialize_deps(BoxPath.dependencies(self._side_effect_paths)):
      self.trigger_saved(btt, temporary_folder)


def serialize_deps(deps: Dict[Any, Set[Any]]) -> List[Any]:
  '''Returns the keys of ``deps`` in an execution order that respects the dependencies.'''
  deps = {k: set(v) for (k, v) in deps.items()}  # Create a copy.
  ordering = []
  while deps:
    for k, v in deps.items():
      if not v:
        ordering.append(k)
        del deps[k]
        for v in deps.values():
          v.discard(k)
        break
    else:
      raise Exception(f'No ordering possible: {deps}')
  return ordering


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


class LynxException(Exception):
  '''Raised when LynxKite indicates that an error has occured while processing a command.'''

  def __init__(self, error):
    super(LynxException, self).__init__(error)
    self.error = error


def _asobject(dic):
  '''Wraps the dict in a namespace for easier access. I.e. d["x"] becomes d.x.'''
  return types.SimpleNamespace(**dic)


def _json_encode(obj):
  if isinstance(obj, types.SimpleNamespace):
    return obj.__dict__
  return obj


def to_simple_dicts(obj):
  '''Converts a SimpleNamespace structure (as returned from LynxKite requests) into simple dicts.'''
  return json.loads(json.dumps(obj, default=_json_encode))


class PizzaBox(LynxKite):

  def __init__(self):
    super().__init__(address='https://pizzabox.lynxanalytics.com/')
    assert self.oauth_token() or self.signed_token(), \
        'Please set LYNXKITE_OAUTH_TOKEN or LYNXKITE_SIGNED_TOKEN.'
