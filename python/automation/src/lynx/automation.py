'''The automation related part of the Python API.

Example usage (the code must be in the dags folder of Airflow)::

    lk = lynx.kite.LynxKite()
    @lk.workspace()
    def trivial():
      return dict(result=lk.createExampleGraph().sql('select name, age from vertices'))
    wss = lynx.automation.WorkspaceSequence(
        ws=trivial,
        schedule=Schedule(pendulum.create(2018, 5, 10, tz=UTC), '* * * * *'),
        lk_root='airflow_test',
        input_recipes=[])
    eg_dag = wss.to_airflow_DAG('eg_dag')
'''

from lynx.kite import LynxKite, State, CustomBox, BoxPath, Workspace, Box
from lynx.kite import _normalize_path, _topological_sort
from collections import OrderedDict
import datetime
import re
import hashlib
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.sensors import BaseSensorOperator
import pendulum
from typing import Dict, List, Any, Set, TypeVar, Optional


UTC = pendulum.timezone('UTC')


def utc_dt(*args, **kwargs) -> pendulum.Pendulum:
  '''Helper function for creating Pendulum UTC datetimes.'''
  return pendulum.create(*args, **kwargs, tz=UTC)


def _assert_is_aware(dt: datetime.datetime):
  '''A datetime object is timezone aware, if it has a tzinfo. In the API we
  require to use Pendulum datetimes.

  For example:

      paris = pendulum.timezone('Europe/Paris')
      dt = pendulum.create(2019, 1, 18, tz=paris)
  '''
  info = '''You have to use pendulum to create datetimes. E.g. pendulum.create(2019, 1, 18)'''
  assert isinstance(dt, pendulum.Pendulum), info


def _assert_is_utc(dt: pendulum.Pendulum):
  '''Checks if dt is a Pendulum UTC datetime.'''
  _assert_is_aware(dt)
  assert dt.timezone_name == 'UTC', 'Timezone is not UTC.'


def _to_pendulum_utc(dt: datetime.datetime) -> pendulum.Pendulum:
  pdt = pendulum.instance(dt)
  # First we test Airflow, whether it still returns UTC dates
  assert pdt.utc, 'Timezone is not UTC.'
  # We need this second instance to make sure that
  # the name of the timezone is "UTC"
  return pendulum.create(pdt.year, pdt.month, pdt.day, pdt.hour, pdt.minute, pdt.second)


def _aware_to_iso_str(dt: pendulum.Pendulum) -> str:
  '''Standard string representation of aware dt.'''
  _assert_is_aware(dt)
  return dt.to_iso8601_string()


class Schedule:
  '''Used to define timezone aware schedulings.

  For example:

      paris = pendulum.timezone('Europe/Paris')
      schedule = Schedule(pendulum.create(2019, 1, 23, tz=paris), '0 4 * * *')

  For detailed information, see:
  https://docs.google.com/document/d/14OOGqBEnVeoZAPGmtd0VhX55qHUVAH1-46UfEACauHE
  '''

  def __init__(self, start_date: pendulum.Pendulum, cron_str: str) -> None:
    _assert_is_aware(start_date)
    self.start_date = start_date
    self.cron_str = cron_str
    # We just use this dag to generate date lists, according to the schedule.
    self.dag = DAG(
        dag_id='to_generate_dates',
        start_date=self.start_date,
        schedule_interval=self.cron_str)

  def assert_utc_dt_is_valid(self, utc_date: pendulum.Pendulum):
    '''Does not raise exception iff utc_date is compatible with cron_str
    and utc_date >= start_date.
    '''
    _assert_is_utc(utc_date)
    assert utc_date >= self.start_date, f'{utc_date} preceeds start date = {self.start_date}'
    dates = self.utc_dates(utc_date, utc_date)
    assert len(dates) == 1, f'Datetime {utc_date} is not valid with this Schedule: {self}.'

  def utc_dates(self,
                utc_start_date: datetime.datetime,
                utc_end_date: datetime.datetime) -> List[datetime.datetime]:
    '''Returns the list of pendulum datetimes, which are valid dates according
    to the cron string of the schedule, and are between utc_start_date and
    utc_end_date. The start date and the end date are also included, if they match
    the cron string.
    '''
    _assert_is_utc(utc_start_date)
    _assert_is_utc(utc_end_date)
    return [_to_pendulum_utc(d) for d in self.dag.get_run_dates(utc_start_date, utc_end_date)]

  def step_back(self, utc_date: datetime.datetime, delta: int) -> datetime.datetime:
    '''Steps back delta times from utc_date on valid schedule dates.

    If delta == 1, it returns the previous schedule date before utc_date.
    '''
    _assert_is_utc(utc_date)
    start_date = utc_date
    for _ in range(delta):
      start_date = self.dag.previous_schedule(start_date)
    return _to_pendulum_utc(start_date)

  def next_date(self, utc_date: datetime.datetime) -> datetime.datetime:
    '''The first valid schedule date after utc_date.'''
    _assert_is_utc(utc_date)
    return _to_pendulum_utc(self.dag.following_schedule(utc_date))

  def __str__(self):
    return f'<start= {self.start_date} cron= {self.cron_str} ({self.start_date.tzinfo})>'


class InputSensor(BaseSensorOperator):
  def __init__(self, input_task, *args, **kwargs):
    super().__init__(*args, **kwargs)
    self.input_task = input_task

  def poke(self, context):
    return self.input_task.is_ready(_to_pendulum_utc(context['execution_date']))


class InputRecipe:
  '''Base class for input recipes.

  Can check whether an input is available, and can build a workspace segment which
  loads the input into a workspace.
  '''

  def is_ready(self, date: datetime.datetime) -> bool:
    raise NotImplementedError()

  def build_boxes(self, date: datetime.datetime) -> State:
    raise NotImplementedError()

  def validate(self, date: datetime.datetime) -> None:
    raise NotImplementedError()


class SnapshotSequence:
  '''A snapshot sequence representing a list of snapshots in LynxKite.

  Attributes:
    location: the LynxKite root directory this snapshot sequence is stored under.
    schedule: a Schedule object, defining the valid timestamps and frequency.
    retention: the time delta after which snapshots can be cleaned up.
    lk: LynxKite connection object.'''

  def __init__(self, lk: LynxKite, location: str, schedule: Schedule,
               retention: datetime.timedelta = None) -> None:
    self.lk = lk
    self._location = location
    self._schedule = schedule
    self._retention = retention

  def _snapshot_name(self, date: datetime.datetime) -> str:
    _assert_is_utc(date)
    return self._location + '/' + _aware_to_iso_str(date)

  def _snapshot_exists(self, date: datetime.datetime) -> bool:
    _assert_is_utc(date)
    entry = self.lk.get_directory_entry(self._snapshot_name(date))
    return entry.exists and entry.isSnapshot

  def create_state_if_available(self, date: datetime.datetime) -> Optional['State']:
    """A fallback if the snapshot is not there yet when a client is asking for it.

    Overwrite this method to return a `State` for the snapshot (or None if you can not) if you
    want to provide such a fallback option. The returned `State` will be saved as a snapshot.
    """
    pass

  def _try_to_create_snapshot_if_not_exist(self, date: datetime.datetime) -> None:
    _assert_is_utc(date)
    if not self._snapshot_exists(date):
      state = self.create_state_if_available(date)
      if state is not None:
        self.lk.save_snapshot(self._snapshot_name(date), self.lk.get_state_id(state))

  def is_ready(self, date: datetime.datetime) -> bool:
    """Checks if the snapshot is already created. If not, it tries to create it and checks if it was
    successful.
    """
    _assert_is_aware(date)
    date = UTC.convert(date)
    self._try_to_create_snapshot_if_not_exist(date)
    return self._snapshot_exists(date)

  def list_dates(self, from_date: datetime.datetime,
                 to_date: datetime.datetime) -> List[datetime.datetime]:
    """Lists all dates matching the cron format `self.cron_str` between `from_date` and
    `to_date`.

    (End points `from_date` and `to_date` included).
    """
    _assert_is_aware(from_date)
    from_date = UTC.convert(from_date)
    _assert_is_aware(to_date)
    to_date = UTC.convert(to_date)
    return self._schedule.utc_dates(from_date, to_date)

  def _snapshots(self, from_date: datetime.datetime, to_date: datetime.datetime) -> List[str]:
    """Lists the name of all the snapshots whose date is between `from_date` and `to_date`.

    Snapshots corresponding `from_date` and `to_date` (if they match the cron format
    `self._schedule.cron_str`) are also listed.
    Also lists snapshots which might not yet exist.
    """
    _assert_is_utc(from_date)
    _assert_is_utc(to_date)
    t = []
    for dt in self.list_dates(from_date, to_date):
      t.append(self._snapshot_name(dt))
    return t

  def read_date(self, date: datetime.datetime) -> 'Box':
    """Returns an importSnapshot box with the corresponding snapshot.

    If the snapshot is not created yet then it first tries to create it. If that doesn't work
    then the importSnapshot box will output an error.
    """
    _assert_is_aware(date)
    date = UTC.convert(date)
    self._try_to_create_snapshot_if_not_exist(date)
    path = self._snapshot_name(date)
    return self.lk.importSnapshot(path=path)

  def remove_date(self, date: datetime.datetime) -> None:
    """Removes the snapshot corresponding to `date` if it exists.

    Does nothing if the snapshot doesn't exist.
    """
    _assert_is_aware(date)
    date = UTC.convert(date)
    path = self._snapshot_name(date)
    self.lk.remove_name(path, force=True)

  def save_to_sequence(self, state_id: str, date: datetime.datetime) -> None:
    ''' Saves a state of id ``state_id`` as a member of the sequence.'''
    # Assert that dt is valid according to the cron_str format.
    _assert_is_aware(date)
    date = UTC.convert(date)
    self._schedule.assert_utc_dt_is_valid(date)
    self.lk.save_snapshot(self._snapshot_name(date), state_id)

  def delete_expired(self, execution_date: datetime.datetime) -> None:
    '''Deletes snapshots that are older than `execution_date - retention`.'''
    _assert_is_utc(execution_date)
    if self._retention:
      threshold = self._snapshot_name(
          execution_date.replace(
              second=0,
              microsecond=0) -
          self._retention)
      for entry in self.lk.list_dir(self._location):
        if entry.name < threshold:
          self.lk.remove_name(entry.name)


class TableSnapshotSequence(SnapshotSequence):
  ''' A special snapshot sequence where all members are of type table. This makes possible
  reading the union of the snapshots for a given interval.'''

  def read_interval(self, from_date: datetime.datetime,
                    to_date: datetime.datetime) -> 'Box':
    """Returns the union of all snapshots between `from_date` and `to_date`.

    If some dates does not exist then it tries to create them on the fly. If it doesn't work
    then the returned box will output an error.
    """
    _assert_is_aware(from_date)
    from_date = UTC.convert(from_date)
    _assert_is_aware(to_date)
    to_date = UTC.convert(to_date)
    dates = self.list_dates(from_date, to_date)
    for dt in dates:
      self._try_to_create_snapshot_if_not_exist(dt)
    paths = ','.join([self._snapshot_name(dt) for dt in dates])
    return self.lk.importUnionOfTableSnapshots(paths=paths)


class TableSnapshotRecipe(InputRecipe):
  '''Input recipe for a table snapshot sequence.
     @param: tss: The TableSnapshotSequence used by this recipe. Can be None, but has to be
             set via set_tss before using this class.
     @param: delta: Steps back delta in time according to the cron string of the tss. Optional,
             if not set this recipe uses the date parameter.
     @param: number_of_snapshots: Gets the union of mentioned number of snapshots until delta date.
             gets a single snapshot by default'''

  def __init__(self, tss: TableSnapshotSequence = None, delta: int = 0,
               number_of_snapshots: int = 1) -> None:
    self.tss = tss
    self.delta = delta
    self.number_of_snapshots = number_of_snapshots

  def set_tss(self, tss: TableSnapshotSequence) -> None:
    assert self.tss is None
    self.tss = tss

  def validate(self, date: datetime.datetime) -> None:
    _assert_is_utc(date)
    assert self.tss, 'TableSnapshotSequence needs to be set.'
    self.tss._schedule.assert_utc_dt_is_valid(date)

  def get_interval(self, date: datetime.datetime):
    assert self.tss, 'TableSnapshotSequence needs to be set.'
    to_date = self.tss._schedule.step_back(date, self.delta)
    from_date = self.tss._schedule.step_back(to_date, self.number_of_snapshots - 1)
    return from_date, to_date

  def is_ready(self, date: datetime.datetime) -> bool:
    _assert_is_utc(date)
    self.validate(date)
    assert self.tss
    from_date, to_date = self.get_interval(date)
    dates = self.tss.list_dates(from_date, to_date)
    return all([self.tss.is_ready(dt) for dt in dates])

  def build_boxes(self, date: datetime.datetime) -> State:
    _assert_is_utc(date)
    self.validate(date)
    assert self.tss
    from_date, to_date = self.get_interval(date)
    return self.tss.read_interval(from_date, to_date)


class RecipeWithDefault(InputRecipe):
  '''Input recipe with a default value.
     @param: src_recipe: The source recipe to use if possible.
     @param: default_date: Provide the default box for this date and src_recipe for later dates.
     @param: default_state: Provide this State for dates earlier than the default date.'''

  def __init__(self, src_recipe: InputRecipe, default_date: datetime.datetime,
               default_state: State) -> None:
    _assert_is_utc(default_date)
    self.src_recipe = src_recipe
    self.default_date = default_date
    self.default_state = default_state

  def validate(self, date: datetime.datetime) -> None:
    _assert_is_utc(date)
    if date != self.default_date:
      self.src_recipe.validate(date)

  def is_ready(self, date: datetime.datetime) -> bool:
    _assert_is_utc(date)
    self.validate(date)
    return date == self.default_date or self.src_recipe.is_ready(date)

  def build_boxes(self, date: datetime.datetime) -> State:
    _assert_is_utc(date)
    self.validate(date)
    if date == self.default_date:
      return self.default_state
    else:
      return self.src_recipe.build_boxes(date)


class Task:
  '''
  The interface that represents a task used for automation.
  '''

  def __init__(self, wss: 'WorkspaceSequence') -> None:
    self._wss = wss
    self._lk = wss.lk

  def _wssi_for_date(self, date: datetime.datetime) -> 'WorkspaceSequenceInstance':
    _assert_is_utc(date)
    return self._wss.wssi_for_date(date)

  def run(self, date: datetime.datetime) -> None:
    '''
    Trigger this endpoint in the workspace instance corresponding to the date parameter.
    '''
    raise NotImplementedError()

  def id(self) -> str:
    '''
    Human-readable ID of the task.
    '''
    raise NotImplementedError()


class BoxTask(Task):
  '''
  A task that is associated with a box on the workspace.
  '''

  def __init__(self, wss: 'WorkspaceSequence', box_path: BoxPath) -> None:
    super().__init__(wss)
    self.box_path = box_path

  def _run_on_instance(self, wss_instance: 'WorkspaceSequenceInstance') -> None:
    raise NotImplementedError()

  def run(self, date: datetime.datetime) -> None:
    _assert_is_utc(date)
    self._run_on_instance(self._wssi_for_date(date))


class Input(BoxTask):
  '''
  A task associated with an input box.
  '''

  def _run_on_instance(self, wss_instance: 'WorkspaceSequenceInstance') -> None:
    input_tss = wss_instance._wss.input_sequences[self.name()]
    if not input_tss._snapshot_exists(wss_instance._date):
      wss_instance.run_input(self.name())

  def id(self) -> str:
    return f'input_{self.name()}'

  def name(self) -> str:
    return self.box_path.base.parameters['name']

  def is_ready(self, date):
    _assert_is_utc(date)
    return self._wss.input_recipes[self.name()].is_ready(date)


class Output(BoxTask):
  '''
  A task associated with an output box.
  '''

  def name(self) -> str:
    return self.box_path.base.parameters['name']

  def _run_on_instance(self, wss_instance: 'WorkspaceSequenceInstance') -> None:
    wss_instance.run_output(self.name())
    wss_instance.delete_expired_output(self.name())

  def id(self) -> str:
    return f'output_{self.name()}'


class Triggerable(BoxTask):
  '''
  A task associated with a triggerable box.
  '''

  def _run_on_instance(self, wss_instance: 'WorkspaceSequenceInstance') -> None:
    wss_instance.trigger(self.box_path)

  def id(self) -> str:
    return 'trigger_' + self.box_path.to_string_id(self._wss.ws)


class SaveWorkspace(Task):
  '''
  A task to save the workspace.
  '''

  def run(self, date: datetime.datetime) -> None:
    _assert_is_utc(date)
    wssi_for_date = self._wssi_for_date(date)
    wssi_for_date.save()

  def id(self) -> str:
    return 'save_workspace'


class RunCleaner(Task):
  '''
  A task to run the cleaner, after the pipeline is completed.
  '''

  def run(self, date: datetime.datetime) -> None:
    _assert_is_utc(date)
    self._wss.lk.move_to_cleaner_trash('notSnapshotEntities')
    self._wss.lk.empty_cleaner_trash()

  def id(self) -> str:
    return 'run_cleaner'


class WorkspaceSequence:
  '''Represents a workspace sequence.

  It can be used in automation to create instances of a workspace for
  timestamps, wrapped in a workspace which can get inputs, and saves outputs.
  '''

  def __init__(self, ws: Workspace, schedule: Schedule, lk_root: str,
               input_recipes: List[InputRecipe], params: Dict[str, Any] = {},
               retention_deltas: Dict[str, datetime.timedelta] = {},
               default_retention: datetime.timedelta = None) -> None:
    self.ws = ws
    self.lk = self.ws.lk
    self._schedule = schedule
    self.local_tz = schedule.start_date.tz
    self.params = params
    self.lk_root = lk_root
    self.input_names = self.ws.inputs  # For the order of the inputs
    self.default_retention = default_retention
    self.input_recipes = dict(zip(self.input_names, input_recipes))
    self.input_sequences: Dict[str, TableSnapshotSequence] = {}
    for inp in self.input_names:
      location = _normalize_path(self.lk_root + '/input-snapshots/' + inp)
      self.input_sequences[inp] = TableSnapshotSequence(self.lk, location, self._schedule)
    self.output_sequences: Dict[str, TableSnapshotSequence] = {}
    for name in retention_deltas.keys():
      assert name in self.ws.outputs, f'{name} is not a valid output name'
    for output in self.ws.outputs:
      location = _normalize_path(self.lk_root + '/output-snapshots/' + output)
      self.output_sequences[output] = TableSnapshotSequence(
          self.lk,
          location,
          self._schedule,
          retention=retention_deltas.get(output, self.default_retention))

  def wssi_for_date(self, date: datetime.datetime) -> 'WorkspaceSequenceInstance':
    '''Returns the WorkspaceSequenceInstance for the given date.

    The WorkspaceSequenceInstance is used to save and run the Workspace with its inputs
    for the given date.'''
    _assert_is_utc(date)
    self._schedule.assert_utc_dt_is_valid(date)
    return WorkspaceSequenceInstance(self, date)

  def ws_for_date(self, date: datetime.datetime) -> 'Workspace':
    '''Returns the wrapper Workspace of the WorkspaceSequenceInstance for the given date.

    If the wrapped ws has a ``date`` workspace parameter, then we will use the
    ``date`` parameter of this method as a value to pass to the workspace.
    '''
    _assert_is_aware(date)
    date = UTC.convert(date)
    wssi = self.wssi_for_date(date)
    return wssi.wrapper_ws()

  def _automation_tasks(self) -> List[Task]:
    inputs: List[Task] = [Input(self, BoxPath(inp)) for inp in self.ws.input_boxes]
    outputs: List[Task] = [Output(self, BoxPath(outp)) for outp in self.ws.output_boxes]
    side_effects: List[Task] = [Triggerable(self, se) for se in self.ws.side_effect_paths()]
    save_ws: List[Task] = [SaveWorkspace(self)]
    run_cleaner: List[Task] = [RunCleaner(self)]
    return inputs + outputs + side_effects + save_ws + run_cleaner

  @staticmethod
  def _add_box_based_dependencies(dag: Dict[Task, Set[Task]]) -> None:
    bp_to_task = {task.box_path: task for task in dag if isinstance(task, BoxTask)}
    for bp, deps in BoxPath.dependencies(bp_to_task.keys()).items():
      dag[bp_to_task[bp]].update(bp_to_task[p] for p in deps)

  @staticmethod
  def _add_save_workspace_deps(dag: Dict[Task, Set[Task]]) -> None:
    save_ws_tasks = [task for task in dag if isinstance(task, SaveWorkspace)]
    num_tasks = len(save_ws_tasks)
    assert num_tasks == 1, f'Only one SaveWorkspace task is expected, but found {num_tasks}'
    save_ws = save_ws_tasks[0]
    for task, deps in dag.items():
      if task != save_ws and not isinstance(task, Input):
        deps.add(save_ws)

  @staticmethod
  def _add_run_cleaner_deps(dag: Dict[Task, Set[Task]]) -> None:
    run_cleaner_tasks = [task for task in dag if isinstance(task, RunCleaner)]
    num_tasks = len(run_cleaner_tasks)
    assert num_tasks == 1, f'Only one RunCleaner task is expected, but found {num_tasks}'
    run_cleaner = run_cleaner_tasks[0]
    dag[run_cleaner] = set()
    for task in dag:
      if task != run_cleaner:
        dag[run_cleaner].add(task)

  def to_dag(self) -> Dict[Task, Set[Task]]:
    '''
    Returns an ordered dict of the tasks and their dependencies to run this workspace
    for a given date. The order of the dict is the topological order of the induced graph.
    '''

    tasks = self._automation_tasks()
    dag: Dict[Task, Set[Task]] = {task: set() for task in tasks}
    self._add_box_based_dependencies(dag)
    self._add_save_workspace_deps(dag)
    self._add_run_cleaner_deps(dag)
    return _minimal_dag(dag)

  def run_dag_tasks(self, date: datetime.datetime) -> None:
    '''
    Runs all the tasks in the generated task DAG for the given execution date,
    in the DAG order.

    Can be used to test, what happens when the wss is automated.
    '''
    _assert_is_utc(date)
    for t in self.to_dag():
      t.run(date)

  def to_airflow_DAG(self, dag_id: str, dag_args={}, task_default_args={}) -> DAG:
    '''
    Creates an Airflow dag from the workspace sequence.

    It can be used in Airflow dag definition files to automate the workspace
    sequence. Airflow task dependencies are defined based on the output of `to_dag`.
    '''

    def hash_end(id: str) -> str:
      return id[:218] + hashlib.md5(id[218:].encode('utf-8')).hexdigest()

    def airflow_allowed_id(raw_id: str) -> str:
      # See: https://github.com/apache/incubator-airflow/blob/master/airflow/utils/helpers.py#L51
      allowed_char_id = re.sub(r'[^0-9a-zA-Z\-\.]', '_', raw_id)
      if len(allowed_char_id) > 250:
        return hash_end(allowed_char_id)
      else:
        return allowed_char_id

    assert not 'start_date' in task_default_args, 'You cannot override start_date.'
    assert not 'owner' in task_default_args, 'You cannot override owner.'
    assert not 'schedule_interval' in dag_args, 'You cannot override schedule_interval.'
    assert not 'default_args' in dag_args, 'Use task_default_args instead.'

    base_default_args = {
        'owner': 'airflow',
        'start_date': self._schedule.start_date,
    }

    dag_parameters = dict(schedule_interval=self._schedule.cron_str, **dag_args)
    task_default_parameters = {**base_default_args, **task_default_args}
    airflow_dag = DAG(
        dag_id,
        default_args=task_default_parameters,
        **dag_parameters)
    task_dag = self.to_dag()
    task_info = {}
    # Creating Airflow operators for tasks.
    for t in task_dag:
      python_op = PythonOperator(
          task_id=airflow_allowed_id(t.id()),
          provide_context=True,
          python_callable=lambda ds, execution_date, t=t, **kwargs: t.run(
              _to_pendulum_utc(execution_date)),
          dag=airflow_dag)
      task_info[t] = dict(id=t.id(), op=python_op)
      if isinstance(t, Input):
        # Adding sensor task for input
        sensor_task_id = f'input_sensor_{t.name()}'
        sensor_op = InputSensor(
            input_task=t,
            task_id=sensor_task_id,
            dag=airflow_dag,
            poke_interval=60,
            timeout=60 * 60 * 12,
            soft_fail=False)
        task_info[t]['op'].set_upstream(sensor_op)
    # Defining dependencies between operators.
    for t in task_dag:
      for dep in task_dag[t]:
        task_info[t]['op'].set_upstream(task_info[dep]['op'])
    return airflow_dag


class WorkspaceSequenceInstance:

  def __init__(self, wss: WorkspaceSequence, date: datetime.datetime) -> None:
    _assert_is_utc(date)
    self._wss = wss
    self._lk = self._wss.lk
    self._date = date

  def base_folder_name(self):
    return _normalize_path(f'{self._wss.lk_root}/workspaces/{_aware_to_iso_str(self._date)}')

  def folder_of_input_workspaces(self):
    return f'{self.base_folder_name()}/inputs'

  def wrapper_folder_name(self):
    return f'{self.base_folder_name()}/main'

  def full_name(self) -> str:
    return f'{self.wrapper_folder_name()}/main'

  def is_saved(self) -> bool:
    path = self.full_name()
    r = self._lk.get_directory_entry(path)
    return r.exists and r.isWorkspace

  def snapshot_path_for_output(self, output: str) -> str:
    return self._wss.output_sequences[output]._snapshot_name(self._date)

  def snapshot_path_for_input(self, name: str) -> str:
    return self._wss.input_sequences[name]._snapshot_name(self._date)

  def wrapper_ws(self) -> Workspace:
    lk = self._lk

    @lk.workspace_with_side_effects(name='main')
    def ws_instance(se_collector):
      inputs = [self._wss.input_sequences[input_name].read_date(self._date)
                for input_name in self._wss.input_names]
      ws = self._wss.ws
      params = self._wss.params
      ws_date_params = {}
      if ws.has_date_parameter():
        # The date parameter will be passed as an ISO-standard UTC string.
        ws_date_params['date'] = _aware_to_iso_str(self._date)
      if ws.has_local_date_parameter():
        local_date = self._wss.local_tz.convert(self._date)
        ws_date_params['local_date'] = _aware_to_iso_str(local_date)
      ws_as_box = ws(*inputs, **params, **ws_date_params)
      ws_as_box.register(se_collector)
      for output in ws.outputs:
        out_path = self.snapshot_path_for_output(output)
        ws_as_box[output].saveToSnapshot(path=out_path).register(se_collector)

    return ws_instance

  def save(self) -> None:
    ws = self.wrapper_ws()
    self._lk.save_workspace_recursively(ws, self.wrapper_folder_name())

  def find(self, box_id_base: str) -> BoxPath:
    """Returns the BoxPath for the box nested in the wrapper workspace whose box_id_base
    is the given string.

    Raises an error if there is not exactly one such a box.
    """
    self_as_box = self.wrapper_ws()()
    return self.wrapper_ws().find(box_id_base).add_box_as_prefix(self_as_box)

  def find_all(self, box_id_base: str) -> List[BoxPath]:
    """Returns the BoxPaths for all boxes nested in the wrapper workspace whose
    box_id_base is the given string.
    """
    self_as_box = self.wrapper_ws()()
    box_paths = self.wrapper_ws().find_all(box_id_base)
    return [bp.add_box_as_prefix(self_as_box) for bp in box_paths]

  def run_input(self, input_name: str) -> None:
    lk = self._lk

    @lk.workspace_with_side_effects(name=input_name)
    def input_ws(se_collector):
      input_state = self._wss.input_recipes[input_name].build_boxes(self._date)
      path = self.snapshot_path_for_input(input_name)
      input_state.saveToSnapshot(path=path).register(se_collector)

    path = f'{self.folder_of_input_workspaces()}/{input_name}'
    lk.remove_name(_normalize_path(path), force=True)
    self._wss.input_sequences[input_name].remove_date(self._date)
    input_ws.save(self.folder_of_input_workspaces())
    input_ws.trigger_all_side_effects()

  def run_all_inputs(self) -> None:
    for input_name in self._wss.input_names:
      self.run_input(input_name)

  def run_output(self, name: str) -> None:
    path = self.snapshot_path_for_output(name)
    self._lk.remove_name(path, force=True)
    ws = self.wrapper_ws()
    for box_path in ws.side_effect_paths():
      if 'path' in box_path.base.parameters and box_path.base.parameters['path'] == path:
        ws.trigger_saved(box_path, self.wrapper_folder_name())
        break
    else:
      raise Exception(f'No output with name {name}')

  def delete_expired_output(self, name: str) -> None:
    self._wss.output_sequences[name].delete_expired(self._date)

  def trigger(self, box_path: BoxPath) -> None:
    '''``box_path`` is relative to the original workspace'''
    wrapper_ws = self.wrapper_ws()
    for box in wrapper_ws.all_boxes:
      if isinstance(box, CustomBox) and box.workspace == self._wss.ws:
        wrapped_ws_as_box = box
        break
    full_box_path = box_path.add_box_as_prefix(wrapped_ws_as_box)
    wrapper_ws.trigger_saved(full_box_path,
                             self.wrapper_folder_name())

  def run(self) -> None:
    '''We trigger all the side effect boxes of the ws.

    This means all the side effects in the wrapped ws and the saving of
    the outputs of the wrapped ws.'''
    self.save()
    # Compute the inputs.
    self.run_all_inputs()
    saved_under_folder = self.wrapper_folder_name()
    # We assume that the same box ids will be generated every time
    # we regenerate this workspace.
    ws = self.wrapper_ws()
    for btt in ws.side_effect_paths():
      ws.trigger_saved(btt, saved_under_folder)


T = TypeVar('T')


def _minimal_dag(g: Dict[T, Set[T]]) -> Dict[T, Set[T]]:
  '''
  This function creates another dependency graph which has the same implicit dependencies
  as the original one (if a depends on b and b depends on c, a implicitly depends on c) but
  is minimal for edge exclusion, i.e. with any explicit dependency deleted the resulting
  graph will miss some implicit or explicit dependency from the original graph. The result
  lists the nodes in topoligical order.

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
  order: List[T] = []
  for group in _topological_sort(g):
    order.extend(group)
    for elem in group:
      deps = g[elem]
      transitive_closure[elem] = deps
      for d in deps:
        transitive_closure[elem] = transitive_closure[elem] | transitive_closure[d]
  min_dag: Dict[T, Set[T]] = OrderedDict()
  for n in order:
    min_dag[n] = set()
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
