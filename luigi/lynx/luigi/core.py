import datetime
from dateutil.relativedelta import relativedelta
import logging
import luigi
import lynx.kite
import re
import subprocess
import os
from . import transfer


_default_connection = lynx.kite.LynxKite()

logger = logging.getLogger('luigi-interface')


def disabling_provider(task):
  ''' Supposed to be overriden if tasks are to be disabled.
      The return value determines if the luigi.Task instance passed in as argument should be
      disabled. So this method should return True when the task is supposed to be disabled.'''
  return False


class ProjectTarget(luigi.target.Target):
  """A target that represents a LynxKite project"""

  def __init__(self, lk, project_name):
    self.project_name = project_name
    # Don't use the lk connection given here for other things
    # than exists() checks. It inherits the authentication from
    # the creating task.
    self._lk_for_exists = lk

  def exists(self):
    entry = self._lk_for_exists.get_directory_entry(self.project_name)
    if entry.exists and not entry.isProject:
      raise Exception('project expected but non-project entry found ', entry)
    return entry.exists

  def load(self, lk):
    return lk.load_project(self.project_name)

  def save(self, project):
    project.save(self.project_name)

  def human_name(self):
    return 'LynxKite project: ' + self.project_name


class TableTarget(luigi.target.Target):
  """A target that represents a LynxKite table"""

  def __init__(self, lk, table_name):
    self.table_name = table_name
    # Don't use the lk connection given here for other things
    # than exists() checks. It inherits the authentication from
    # the creating task.
    self._lk_for_exists = lk

  def exists(self):
    entry = self._lk_for_exists.get_directory_entry(self.table_name)
    if entry.exists and not entry.isTable:
      raise Exception('table expected but non-table entry found ', entry)
    return entry.exists

  def load(self, lk):
    return lk.load_table(self.table_name)

  def save(self, table):
    table.save(self.table_name)

  def human_name(self):
    return 'LynxKite table: ' + self.table_name


class ViewTarget(luigi.target.Target):
  """A target that represents a LynxKite view"""

  def __init__(self, lk, view_name):
    self.view_name = view_name
    # Don't use the lk connection given here for other things
    # than exists() checks. It inherits the authentication from
    # the creating task.
    self._lk_for_exists = lk

  def exists(self):
    entry = self._lk_for_exists.get_directory_entry(self.view_name)
    if entry.exists and not entry.isView:
      raise Exception('view expected but non-view entry found ', entry)
    return entry.exists

  def load(self, lk):
    return lk.load_view(self.view_name)

  def view(self, lk):
    return self.load(lk)

  def save(self, view):
    view.save(self.view_name)

  def human_name(self):
    return 'LynxKite view: ' + self.view_name


class LynxTask(luigi.Task):
  """A task that has a LynxKite connection."""

  @property
  def disabled(self):
    return disabling_provider(self)

  # You can specify another connection by setting lk in
  # your task definition.
  lk = _default_connection

  def inputview(self):
    """LynxTableFileTarget is a common input for LynxTasks. Returns the view(s) for this input type.
    The output type of this function can be a single task, a dict of tasks or a list of tasks
    depending on the requires function."""
    input = self.input()
    if isinstance(input, LynxTableFileTarget):
      return self.input().view(self.lk)
    elif isinstance(input, dict):
      return {name: target.view(self.lk) for name, target in input.items()}
    else:
      return [i.view(self.lk) for i in input]

  def inputviews(self):
    """Deprecated, please use inputview()."""
    return self.inputview()


class PrefixedFileTask(LynxTask):
  """A LynxTask that outputs a PrefixedTarget.

  The subclass is still responsible for creating the output file. This class just makes it a bit
  easier to declare the output. For example::

    class MyTask(PrefixedFileTask):
      def prefixed_output(self):
        return 'DATA$/export/foo.csv'

      def run(self):
        data = self.lk.sql('select * from x where y="z"', x=self.inputview())
        data.export_csv(self.prefixed_output())
  """

  def prefixed_output(self):
    """Override this to specify the prefixed file path."""
    raise NotImplementedError

  def output(self):
    return PrefixedTarget(self.lk, self.prefixed_output())


class PrefixedTarget(luigi.target.Target):
  """A prefixed file target. Can be used with TransferTask."""

  def __init__(self, lk, path):
    self.path = path
    # Don't use the lk connection given here for other things
    # than exists() checks. It inherits the authentication from
    # the creating task.
    self._lk_for_exists = lk

  def exists(self):
    """Returns True if the file exists."""
    return self._lk_for_exists.get_prefixed_path(self.path + '/_SUCCESS').exists

  def resolved_path(self):
    """Returns the resolved path for the file."""
    return self._lk_for_exists.get_prefixed_path(self.path).resolved

  def human_name(self):
    return self.path

  def copy(self, destination):
    """Copies the file to the given prefixed path."""
    self.copy_resolved(self._lk_for_exists.get_prefixed_path(destination).resolved)

  def copy_resolved(self, destination):
    """Copies the file to the given resolved path."""
    transfer.transfer(
        self.transfer_target(),
        transfer.HDFSTarget(destination))

  def transfer_target(self):
    return transfer.HDFSTarget(self.resolved_path())


class LynxTableFileTarget(PrefixedTarget):
  """
  A target for easily importing and exporting tabular
  data between LynxKite views and external files.
  Currently the Parquet format is used.
  """

  def __init__(self, lk, name, shuffle_partitions=None, ttl=None):
    if ttl:
      path = 'DATA$/table_files/{} (ttl={})'.format(name, ttl)
    else:
      path = 'DATA$/table_files/{}'.format(name)
    super().__init__(lk, path)
    self.shuffle_partitions = shuffle_partitions

  def export(self, view):
    view.export_parquet(path=self.path, shuffle_partitions=self.shuffle_partitions)

  def view(self, lk):
    return lk.import_parquet(self.path)


class LynxTableFileTask(LynxTask):
  """
  A task whose output is written to a file. Subclasses should
  override ``compute_view()`` to return a LynxKite view.

  LynxTableFileTask defines a ``ttl`` attribute. Set it to a duration specified in hours, days, or
  minutes (for example ``48h``, ``7d``, ``1h30m``) in your subclass. The output file will be deleted
  by a periodic cleanup task (ValkyrieCleanup) once it becomes older than the set duration. If
  ``ttl`` is not set, the output file will never be automatically deleted.

  Usage example::

    class MyTask(LynxTableFileTask):
      def compute_view(self):
        return self.lk.import_jdbc(JDBC_URL, 'my_table')
  """
  ttl = None
  shuffle_partitions = None

  def compute_view(self):
    """Override this to specify behavior."""
    raise NotImplementedError

  def output_name(self):
    """Optionally override this to specify the name of the output file."""
    return self.task_id

  def output(self):
    return LynxTableFileTarget(self.lk, self.output_name(),
                               shuffle_partitions=self.shuffle_partitions,
                               ttl=self.ttl)

  def run(self):
    result = self.compute_view()
    self.output().export(result)


class SchemaEnforcedTask(LynxTableFileTask):
  """
  A schema enforced task.

  Raises an error if the schema of its ``compute_view()`` method has been altered
  since the first run.

  Usage example::
    class MyTask(SchemaEnforcedTask)
  """
  schema_directory = None

  def run(self):
    result = self.compute_view()
    if self.schema_directory:
      schema_file = self.schema_directory + '/' + self.task_family + '.schema'
      result.enforce_schema(schema_file)
    self.output().export(result)


class ProjectTask(LynxTask):
  """
  A LynxTask that outputs a LynxKite project. Subclasses should
  override ``compute_project()`` to return the project.

  Usage example::

    class MyTask(ProjectTask):
      def compute_project(self):
        return self.lk.new_project().examplegraph()
  """

  def compute_project(self):
    """Override this to return project."""
    raise NotImplementedError

  def output_name(self):
    """Optionally override this to specify the name of the output project."""
    return 'luigi_projects/' + self.task_id

  def output(self):
    return ProjectTarget(self.lk, self.output_name())

  def run(self):
    p = self.compute_project()
    p.compute()
    self.output().save(p)


class TableTask(LynxTask):
  """
  A LynxTask that outputs a LynxKite table. Subclasses must
  override ``compute_view()`` to return the table as a view
  or override ``compute_table()`` to return it as a table.

  Usage example::

    class MyTask(TableTask):
      def compute_view(self):
        return self.lk.import_csv(prefixed_path, infer=False)
  """

  def compute_view(self):
    """Override this to return table as view."""
    raise NotImplementedError

  def compute_table(self):
    """Override this to return table as table."""
    return self.compute_view().to_table()

  def output_name(self):
    """Optionally override this to specify the name of the output table."""
    return 'luigi_tables/' + self.task_id

  def output(self):
    return TableTarget(self.lk, self.output_name())

  def run(self):
    t = self.compute_table()
    t.compute()
    self.output().save(t)


class ViewTask(LynxTask):
  """
  A LynxTask that outputs a LynxKite view. Subclasses must
  override ``compute_view()``.

  Usage example::

    class MyTask(ViewTask):
      def compute_view(self):
        return self.lk.import_csv(prefixed_path, infer=False)
  """

  def compute_view(self):
    """Override this to return the desired view."""
    raise NotImplementedError

  def output_name(self):
    """Optionally override this to specify the name of the output view."""
    return 'luigi_views/' + self.task_id

  def output(self):
    return ViewTarget(self.lk, self.output_name())

  def run(self):
    t = self.compute_view()
    self.output().save(t)


class GenericJDBCTask(LynxTableFileTask):
  """
  A task that extracts 1 table from a database.
  Child classes only need to override ``jdbc_url``, ``table``, ``sql``, and ``key_column``
  to work. If the table has no key column, then ``key_column`` can be the empty string, but then
  the extraction will happen in a single partition.

  If you want to modify the properties of the jdbc connection, you can use ``properties``. This is
  needed for example in MySQL, because the `fetchsize` attribute needs to be set to -2147483648
  (Integer.MIN_VALUE) to stream rows instead of loading them all to memory.

  The query (in the ``sql`` attribute) cannot refer to task parameters directly. If you want the
  query to be different from one task instance to the other, you can use string substitutions in it.
  Use the ``custom_parameters`` method to define such substitutions.

  ``GenericJDBCTask`` by default defines a ``date`` parameter and makes it available for
  substitution under the ``date`` name. See the examples and the :class:`SuperDate` class for tips
  on using this flexible parameter.

  Example subclasses that rely on this::

    class MyJDBCTable(lynx.luigi.GenericJDBCTask):
      jdbc_url = 'jdbc:mysql://localhost/test'
      table = 'my_table'
      sql = "SELECT id, date FROM my_table WHERE '{date.yesterday}' <= date AND date <= '{date}'"
      properties = { 'fetchsize': 10000 }

    class MyJDBCNative(lynx.luigi.GenericJDBCTask):
      jdbc_url = 'jdbc:mysql://localhost/test'
      native_sql = 'SELECT * FROM my_table WHERE {date.minus1year.mysql_timestamp} < date'
      sql = "SELECT id, date FROM source_table WHERE month = '{date:%m}'"

  In the second example the ``source_table`` name is compulsory. In the first case the original
   table name, ``my_table``, and ``source_table`` are accepted in the sql statement.
  """
  date = luigi.DateMinuteParameter()
  table = None
  native_sql = None
  properties = {}
  enable_teradata_schema_auto_fix = False

  def __init__(self, *vargs, **kwargs):
    if (self.table is None) and (self.native_sql is None):
      raise ValueError('You have to define table OR native_sql.')
    if (self.table is not None) and (self.native_sql is not None):
      raise ValueError('You can only define EITHER table OR native_sql.')
    super().__init__(*vargs, **kwargs)

  def get_parameters(self):
    sd = SuperDate(self.date)
    parameters = dict(date=sd)
    parameters.update(self.custom_parameters())
    return parameters

  def custom_parameters(self):
    """
    Override this if you need custom parameters, e.g.::

      return dict(experiment_id=self.experiment_id)
    """
    return {}

  def sql_gen(self):
    return self.sql.format(**self.get_parameters())

  def compute_view(self):
    if self.table is not None:
      table_or_native = self.table
      logger.info('{task_id}: fetching table from native database: {table}'.format(
          task_id=self.task_id,
          table=self.table))
    else:
      native_sql = self.native_sql.format(**self.get_parameters())
      table_or_native = '(' + native_sql + ') source_table'
      if self.enable_teradata_schema_auto_fix:
        table_or_native += '/*LYNX-TD-SCHEMA-AUTO-FIX*/'
      logger.info('{task_id}: issuing native database query: {sql}'.format(
          task_id=self.task_id,
          sql=native_sql))
    imported = self.lk.import_jdbc(
        jdbcUrl=self.jdbc_url,
        jdbcTable=table_or_native,
        keyColumn=self.key_column,
        properties=self.properties)
    sql = self.sql_gen()
    registrations = {'source_table': imported}
    if self.table is not None:
      registrations[self.table] = imported
    return self.lk.sql(sql, **registrations)


class SuperDate:
  '''A wrapper for Python's ``datetime`` for convenient use in :class:`GenericJDBCTask` queries.

  It provides convenient access to relative dates. E.g. ``{date.minus1year}`` gives you the same
  date a year earlier. These relative dates can be combined without limit, e.g.
  ``{date.plus1week.minus3months.plus2minutes.oracle_timestamp}``.

  The default format for the class is ``YYYY-MM-DD`` or using the `formal syntax
  <https://docs.python.org/3/library/datetime.html#strftime-strptime-behavior>`_, ``%Y-%m-%d``.
  You can use a different format with the following syntax: ``{date:%m/%d/%y}``. This is also useful
  if you just want to access one part of the date (e.g. ``{date:%d}``).
  '''

  def __init__(self, date):
    self.date = date

  def __format__(self, spec):
    if not spec:
      spec = '%Y-%m-%d'
    return self.date.__format__(spec)

  @property
  def yesterday(self):
    '''Equivalent to ``minus1day``.'''
    return self.minus1day

  @property
  def oracle_timestamp(self):
    '''Oracle native format. The time part (``00:00``) is zeroed out.'''
    return "to_timestamp('{:%Y-%m-%d} 00:00:00', 'YYYY-MM-DD HH24:MI:SS')".format(self)

  @property
  def teradata_timestamp(self):
    '''Teradata native format. The time part (``00:00``) is zeroed out.'''
    return "CAST ('{:%Y-%m-%d}' AS TIMESTAMP(0) FORMAT 'YYYY-MM-DD')".format(self)

  @property
  def db2_timestamp(self):
    '''DB2 native format. The time part (``00:00``) is zeroed out.'''
    return "TIMESTAMP('{:%Y-%m-%d} 00:00:00')".format(self)

  @property
  def mysql_timestamp(self):
    '''MySQL native format. The time part (``00:00``) is zeroed out.'''
    return "TIMESTAMP '{:%Y-%m-%d} 00:00:00'".format(self)

  @property
  def unix_timestamp(self):
    return str(int(self.date.timestamp()))

  @property
  def sparksql_timestamp(self):
    '''Spark SQL format.'''
    return 'CAST(FROM_UNIXTIME({ts}) AS TIMESTAMP)'.format(
        ts=self.unix_timestamp)

  @property
  def sparksql_date(self):
    return 'CAST(FROM_UNIXTIME({ts}) AS DATE)'.format(
        ts=self.unix_timestamp)

  def __getattr__(self, attr):
    if attr.startswith('plus'):
      return SuperDate(self.date + SuperDate._parse_delta(attr[len('plus'):]))
    elif attr.startswith('minus'):
      return SuperDate(self.date - SuperDate._parse_delta(attr[len('minus'):]))
    else:
      return self.__getattribute__(attr)

  _delta_parser = re.compile(r'(\d+)(year|month|week|day|hour|minute|second)s?')

  @staticmethod
  def _parse_delta(s):
    m = SuperDate._delta_parser.match(s)
    assert m, 'Could not parse: {}'.format(s)
    amount, unit = m.groups()
    return relativedelta(**{unit + 's': int(amount)})
