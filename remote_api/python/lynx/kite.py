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
import inspect

if sys.version_info.major < 3:
  raise Exception('At least Python version 3 is needed!')


def timestamp_is_valid(dt, cron_str):
  '''Checks whether ``dt`` is valid according to cron_str.'''
  i = croniter(cron_str, dt - datetime.timedelta(seconds=1))
  return i.get_next(datetime.datetime) == dt


def step_back(cron_str, date, delta):
  cron = croniter(cron_str, date)
  start_date = date
  for _ in range(delta):
    start_date = cron.get_prev(datetime.datetime)
  return start_date


def random_ws_folder():
  return 'tmp_workspaces/{}'.format(
      ''.join(random.choice('0123456789ABCDEF') for i in range(16)))


class TableSnapshotSequence:
  '''A snapshot sequence representing a list of table type snapshots in LynxKite.

  Attributes:
    location (str): the LynxKite root directory this snapshot sequence is stored under.
    cron_str (str): the Cron format defining the valid timestamps and frequency.'''

  def __init__(self, location, cron_str):
    self._location = location
    self.cron_str = cron_str

  def snapshot_name(self, date):
    # TODO: make it timezone independent
    return self._location + '/' + str(date)

  def snapshots(self, lk, from_date, to_date):
    # We want to include the from_date if it matches the cron format.
    i = croniter(self.cron_str, from_date - datetime.timedelta(seconds=1))
    t = []
    while True:
      dt = i.get_next(datetime.datetime)
      if dt > to_date:
        break
      t.append(self.snapshot_name(dt))
    return t

  def read_interval(self, lk, from_date, to_date):
    paths = ','.join(self.snapshots(lk, from_date, to_date))
    return lk.importUnionOfTableSnapshots(paths=paths)

  def read_date(self, lk, date):
    path = self.snapshots(lk, date, date)[0]
    return lk.importSnapshot(path=path)

  def save_to_sequence(self, lk, table_state, dt):
    # Assert that dt is valid according to the cron_str format.
    assert timestamp_is_valid(dt, self.cron_str), "Datetime %s does not match cron format %s." % (
        dt, self.cron_str)
    lk.save_snapshot(self.snapshot_name(dt), table_state)


def _python_name(name):
  '''Transforms a space separated string into a camelCase format.

  The operation "Use base project as segmentation" will be called as
  ``useBaseProjectAsSegmentation``. Dashes are ommitted.
  '''
  name = ''.join([c if c.isalnum() or c == ' ' else '' for c in name])
  return ''.join(
      [x.lower() for x in name.split()][:1] +
      [x[:1].upper() + x[1:] for x in name.split()][1:])


_anchor_box = {
    'id': 'anchor',
    'operationId': 'Anchor',
    'parameters': {},
    'x': 0, 'y': 0,
    'inputs': {},
    'parametricParameters': {}
}


class WorkspaceParameter:
  ''' Represents a workspace parameter declaration.'''

  def __init__(self, name, kind, default_value=''):
    self.name = name
    self.kind = kind
    self.default_value = default_value

  def to_json(self):
    return dict(id=self.name, kind=self.kind, defaultValue=self.default_value)


def text(name, default=''):
  '''Helper function to make it easy to define a text kind ws parameter.'''
  return WorkspaceParameter(name, 'text', default_value=default)


class ParametricParameter:
  '''Represents a parametric parameter value. It should be a string.'''

  def __init__(self, parametric_expr):
    self._value = parametric_expr

  def __str__(self):
    return self._value


pp = ParametricParameter


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
          self.box.bc, self.box.lk, name, inputs={input_name: self}, parameters=kwargs)

    if not name in self.box.bc.box_names():
      raise AttributeError('{} is not defined on {}'.format(name, self))
    return f

  def __dir__(self):
    return super().__dir__() + self.box.bc.box_names()

  def __str__(self):
    return "Output {} of box {}".format(self.output_plug_name, self.box)

  def sql(self, sql, **kwargs):
    return self.sql1(sql=sql, **kwargs)

  def df(self, limit=-1):
    '''Returns a Pandas DataFrame if this state is a table.'''
    import pandas
    table = self.get_table_data(limit)
    header = [c.name for c in table.header]
    data = [[getattr(c, 'double', c.string) if c.defined else None for c in r] for r in table.data]
    return pandas.DataFrame(data, columns=header)

  def get_table_data(self, limit=-1):
    '''Returns the "raw" table data if this state is a table.'''
    return self.box.lk.get_table_data(self.box.lk.get_state_id(self), limit)

  def get_project(self):
    '''Returns the project metadata if this state is a project.'''
    return self.box.lk.get_project(self.box.lk.get_state_id(self))

  def run_export(self):
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

  def compute(self):
    '''Triggers the computation of this state.

    Uses a temporary folder to save a temporary workspace for this computation.
    '''
    folder = random_ws_folder()
    folder_with_slash = folder + '/'
    name = 'tmp_ws_name'
    box = self.computeInputs()
    lk = self.box.lk
    ws = Workspace(name, [box])
    lk.save_workspace_recursively(ws, folder_with_slash)
    lk.trigger_box(folder_with_slash + name, 'box_0')
    # We need the folder name without the trailing '/'.
    lk.remove_name(folder, force=True)

  def save_snapshot(self, path):
    '''Save this state as a snapshot under path.'''
    lk = self.box.lk
    state_id = lk.get_state_id(self)
    lk.save_snapshot(path, state_id)

  def save_to_sequence(self, tss, date):
    '''Save this state to the ``tss`` TableSnapshotSequence with ``date`` as
    the date of the snapshot.'''
    lk = self.box.lk
    state_id = lk.get_state_id(self)
    tss.save_to_sequence(lk, state_id, date)


def new_box(bc, lk, operation, inputs, parameters):
  if isinstance(operation, str):
    outputs = bc.outputs(operation)
  else:
    outputs = operation.outputs()
  if len(outputs) == 1:
    # Special case: single output boxes function as state as well.
    return SingleOutputBox(bc, lk, operation, outputs[0], inputs, parameters)
  else:
    return Box(bc, lk, operation, inputs, parameters)


class Box:
  '''Represents a box in a workspace segment.

  It can store workspace segments, connected to its input plugs.
  '''

  def __init__(self, box_catalog, lk, operation, inputs, parameters):
    self.bc = box_catalog
    self.lk = lk
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
    self.parameters = {}
    self.parametric_parameters = {}
    # We separate normal and parametric parameters here.
    # Parametric parameters can be specified as `name=PP('parametric value')`
    for key, value in parameters.items():
      if isinstance(value, ParametricParameter):
        self.parametric_parameters[key] = str(value)
      else:
        self.parameters[key] = str(value)

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
        self.operation,
        self.parameters,
        self.inputs)


class SingleOutputBox(Box, State):

  def __init__(self, box_catalog, lk, operation, output_name, inputs, parameters):
    Box.__init__(self, box_catalog, lk, operation, inputs, parameters)
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

  def __init__(self, name, terminal_boxes, input_boxes=[], ws_parameters=[]):
    '''The workspace parameter declarations can be specified as a list:
    ``ws_parameters = [text('alma'), text('korte', 'default_for_korte')]``
    '''
    self._name = name or 'Anonymous'
    self._all_boxes = set()
    self._box_ids = dict()
    self._next_id = 0
    self._inputs = [inp.parameters['name'] for inp in input_boxes]
    self._outputs = [
        outp.parameters['name'] for outp in terminal_boxes
        if outp.operation == 'output']
    self._bc = terminal_boxes[0].bc
    self._ws_parameters = ws_parameters
    self._terminal_boxes = terminal_boxes
    self._lk = terminal_boxes[0].lk

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

  def _ws_parameters_to_str(self):
    return json.dumps([param.to_json() for param in self._ws_parameters])

  def to_json(self, workspace_root):
    normal_boxes = [
        box.to_json(self.id_of, workspace_root) for box in self._all_boxes]
    # We use ws_parameters to customize _anchor_box.
    ab = copy.deepcopy(_anchor_box)
    ab['parameters'] = dict(parameters=self._ws_parameters_to_str())
    return [ab] + normal_boxes

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

  def has_date_parameter(self):
    return 'date' in [p.name for p in self._ws_parameters]

  def terminal_box_ids(self):
    return [self.id_of(box) for box in self._terminal_boxes]

  def __call__(self, *args, **kwargs):
    inputs = dict(zip(self.inputs(), args))
    return new_box(self._bc, self._lk, self, inputs=inputs, parameters=kwargs)


class WorkspaceSequence:
  '''Represents a workspace sequence.

  It can be used in automation to create instances of a workspace for
  timestamps, wrapped in a workspace which can get inputs, and saves outputs.
  '''

  def __init__(self, ws, schedule, start_date, params, lk_root, dfs_root, input_recipes):
    self._ws = ws
    self._schedule = schedule
    self._start_date = start_date
    self._params = params
    self._lk_root = lk_root
    self._dfs_root = dfs_root
    self._input_recipes = input_recipes
    self._output_sequences = {}
    for output in self._ws.outputs():
      self._output_sequences[output] = TableSnapshotSequence(self._lk_root + output, self._schedule)

  def output_sequences(self):
    '''Returns the output sequences of hte workspace sequence as a dict.'''
    return self._output_sequences

  def _wrapper_name(self, date):
    return '{}_wrapper_for_{}'.format(self._ws.name(), date)

  def ws_for_date(self, lk, date):
    '''If the wrapped ws has a ``date`` workspace parameter, then we will use the
    ``date`` parameter of this method as a value to pass to the workspace. '''
    assert date >= self._start_date, "{} preceeds start date = {}".format(date, self._start_date)
    assert timestamp_is_valid(
        date, self._schedule), "{} is not valid according to {}".format(date, self._schedule)
    return WorkspaceSequenceInstance(self, lk, date)

  def lk_root(self):
    return self._lk_root


class WorkspaceSequenceInstance:

  def __init__(self, wss, lk, date):
    self._wss = wss
    self._lk = lk
    self._date = date

  def full_name(self):
    return self._wss.lk_root() + self._wss._wrapper_name(self._date)

  def is_saved(self):
    path = self.full_name()
    r = self._lk.get_directory_entry(path)
    return r.exists and r.isWorkspace

  def save(self):
    '''I also runs the imports.'''
    assert not self.is_saved(), 'WorkspaceSequenceInstance is already saved.'
    inputs = [
        input_recipe.build_boxes(self._lk, self._date)
        for input_recipe in self._wss._input_recipes]
    ws_as_box = (
        self._wss._ws(*inputs, **self._wss._params, date=self._date) if self._wss._ws.has_date_parameter()
        else self._wss._ws(*inputs, **self._wss._params))
    terminal_boxes = []
    for output in self._wss._ws.outputs():
      out_path = self._wss._output_sequences[output].snapshot_name(self._date)
      terminal_boxes.append(ws_as_box[output].saveToSnapshot(path=out_path))
    ws = Workspace(self._wss._wrapper_name(self._date), terminal_boxes)
    self._lk.save_workspace_recursively(ws, self._wss.lk_root())

  def run(self):
    '''We trigger all the terminal boxes of the wrapped ws.

    First we just trigger ``saveToSnapshot`` boxes.
    '''
    if not self.is_saved():  # WorkspaceSequenceInstance has to be saved to be able to run.
      self.save()
    operations_to_trigger = ['Save to snapshot']  # TODO: add compute and exports
    full_name = self.full_name()
    boxes = self._lk.get_workspace(full_name)
    terminal_box_ids = [box.id for box in boxes if box.operationId in operations_to_trigger]
    for box_id in terminal_box_ids:
      self._lk.trigger_box(full_name, box_id)


class InputRecipe:
  '''Base class for input recipes.

  Can check whether an input is available, and can build a workspace segment which
  loads the input into a workspace.
  '''

  def is_ready(self, lk, date):
    raise NotImplementedError()

  def build_boxes(self, lk, date):
    raise NotImplementedError()

  def validate(self, date):
    raise NotImplementedError()


class TableSnapshotRecipe(InputRecipe):
  '''Input recipe for a table snapshot sequence.
     @param: tss: The TableSnapshotSequence used by this recipe. Can be None, but has to be
             set via set_tss before using this class.
     @param: delta: Steps back delta in time according to the cron string of the tss. Optional,
             if not set this recipe uses the date parameter.'''

  def __init__(self, tss=None, delta=0):
    self.tss = tss
    self.delta = delta

  def set_tss(self, tss):
    assert self.tss is None
    self.tss = tss

  def validate(self, date):
    assert self.tss, 'TableSnapshotSequence needs to be set.'
    assert timestamp_is_valid(
        date, self.tss.cron_str), '{} does not match {}.'.format(date, self.tss.cron_str)
    return True

  def is_ready(self, lk, date):
    self.validate(date)
    adjusted_date = step_back(self.tss.cron_str, date, self.delta)
    r = lk.get_directory_entry(self.tss.snapshot_name(adjusted_date))
    return r.exists and r.isSnapshot

  def build_boxes(self, lk, date):
    self.validate(date)
    adjusted_date = step_back(self.tss.cron_str, date, self.delta)
    return self.tss.read_interval(lk, adjusted_date, adjusted_date)


class RecipeWithDefault(InputRecipe):
  '''Input recipe with a default value.
     @param: src_recipe: The source recipe to use if possible.
     @param: default_date: Provide the default box for this date and src_recipe for later dates.
     @param: default_state: Provide this State for dates earlier than the default date.'''

  def __init__(self, src_recipe, default_date, default_state):
    self.src_recipe = src_recipe
    self.default_date = default_date
    self.default_state = default_state

  def validate(self, date):
    assert (date == self.default_date) or self.src_recipe.validate(date)
    return True

  def is_ready(self, lk, date):
    self.validate(date)
    return date == self.default_date or self.src_recipe.is_ready(lk, date)

  def build_boxes(self, lk, date):
    self.validate(date)
    if date == self.default_date:
      return self.default_state
    else:
      return self.src_recipe.build_boxes(lk, date)


def layout(boxes):
  '''Compute coordinates of boxes in a workspace.

  The workspace is given as a list of boxes. The return value is a list of
  new boxes, where the coordinates are filled in.
  '''
  dx = 200
  dy = 200
  ox = 150
  oy = 150

  def topological_sort(dependencies):
    # We have all the boxes as keys in the parameter,
    # dependencies[box_id] = {}, if box_id does not depend on anything.
    deps = dependencies.copy()
    while True:
      next_group = set(box_id for box_id, dep in deps.items() if len(dep) == 0)
      if not next_group:
        break
      yield next_group
      deps = {box_id: dep - next_group for box_id, dep in deps.items() if box_id not in next_group}

  dependencies = {box['id']: set() for box in boxes}
  level = {}
  for box in boxes:
    current_box = box['id']
    for name, inp in box['inputs'].items():
      input_box = inp['boxId']
      dependencies[current_box].add(input_box)

  cur_level = 0
  groups = list(topological_sort(dependencies))
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


class LynxKite:
  '''A connection to a LynxKite instance.

  Some LynxKite API methods take a connection argument which can be used to communicate with
  multiple LynxKite instances from the same session. If no arguments to the constructor are
  provided, then a connection is created using the following environment variables:
  ``LYNXKITE_ADDRESS``, ``LYNXKITE_USERNAME``, ``LYNXKITE_PASSWORD``,
  ``LYNXKITE_PUBLIC_SSL_CERT``, ``LYNXKITE_OAUTH_TOKEN``.
  '''

  def __init__(self, username=None, password=None, address=None,
               certfile=None, oauth_token=None, box_catalog=None):
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
    self._box_catalog = box_catalog  # TODO: create standard offline box catalog

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
      box = new_box(self.box_catalog(), self, name, inputs=inputs, parameters=kwargs)
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

  def sql(self, sql, *args, **kwargs):
    '''Shorthand for sql1, sql2, ..., sql10 boxes'''
    num_inputs = len(args)
    assert num_inputs > 0, 'SQL needs at least one input.'
    assert num_inputs < 11, 'SQL can have at most ten inputs.'
    name = 'sql{}'.format(num_inputs)
    inputs = dict(zip(self.box_catalog().inputs(name), args))
    kwargs['sql'] = sql
    return new_box(self.box_catalog(), self, name, inputs=inputs, parameters=kwargs)

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

  def save_workspace_recursively(self, ws, save_under_root=None):
    ws_root = save_under_root
    if ws_root is None:
      ws_root = random_ws_folder() + '/'
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
      self.save_workspace(
          ws_root + rws.name(), layout(rws.to_json(ws_root)))
    if save_under_root is not None:
      self.save_workspace(
          save_under_root + ws.name(), layout(ws.to_json(save_under_root)))
    # If saved, we return the full name of the main workspace also.
    return ws_root, (save_under_root is not None) and save_under_root + ws.name()

  def run_workspace(self, ws, save_under_root=None):
    ws_root, _ = self.save_workspace_recursively(ws, save_under_root)
    return self.run(ws.to_json(ws_root))
    # TODO: clean up saved workspaces if save_under_root is not set.

  def get_state_id(self, state):
    ws = Workspace('Anonymous', [state.box])
    workspace_outputs = self.run_workspace(ws)
    box_id = ws.id_of(state.box)
    plug = state.output_plug_name
    output = workspace_outputs[box_id, plug]
    assert output.success.enabled, 'Output `{}` of `{}` has failed: {}'.format(
        plug, box_id, output.success.disabledReason)
    return output.stateId

  def get_scalar(self, guid):
    return self._ask('/ajax/scalarValue', dict(scalarId=guid))

  def get_project(self, state, path=''):
    return self._ask('/ajax/getProjectOutput', dict(id=state, path=path))

  def get_export_result(self, state):
    return self._ask('/ajax/getExportResultOutput', dict(stateId=state))

  def get_table_data(self, state, limit=-1):
    return self._ask('/ajax/getTableOutput', dict(id=state, sampleRows=limit))

  def get_workspace(self, path):
    response = self._ask('/ajax/getWorkspace', dict(top=path, customBoxStack=[]))
    return response.workspace.boxes

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

  def workspace(self, name=None, parameters=[]):
    def ws_decorator(builder_fn):
      real_name = builder_fn.__name__ if not name else name
      inputs = [self.input(name=name)
                for name in inspect.signature(builder_fn).parameters.keys()]
      results = builder_fn(*inputs)
      outputs = [state.output(name=name) for name, state in results.items()]
      return Workspace(real_name, outputs, inputs, parameters)
    return ws_decorator

  def trigger_box(self, workspace_name, box_id):
    '''Trigger the computation of all the GUIDs in the box which is in the
    saved workspace named ``workspace_name`` and has ``boxID=box_id``.
    '''
    return self._send(
        '/ajax/triggerBox',
        dict(workspace=dict(top=workspace_name, customBoxStack=[]), box=box_id))


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
