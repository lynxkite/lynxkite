'''Python interface for the LynxKite Remote API.

The default LynxKite connection parameters can be configured through the following environment
variables::

    LYNXKITE_ADDRESS=https://lynxkite.example.com/
    LYNXKITE_USERNAME=user@company
    LYNXKITE_PASSWORD=my_password
    LYNXKITE_PUBLIC_SSL_CERT=/tmp/lynxkite.crt

Example usage::

    import lynx
    lk = lynx.LynxKite()
    p = lk.new_project()
    p.newVertexSet(size=100)
    print(p.scalar('vertex_count'))

The list of operations is not documented, but you can copy the invocation from a LynxKite project
history.
'''
import collections
import json
import os
import requests
import ssl
import sys
import types
import yaml

if sys.version_info.major < 3:
  raise Exception('At least Python version 3 is needed!')

default_sql_limit = 1000
default_privacy = 'public-read'


class DirectoryEntry:
  '''name: str

  type: (``'project'``, ``'view'``, ``'table'``, or ``'directory'``)

  checkpoint: str

  object: the Python API object representing each entry. Directories have a ``list`` convenience
  method for easier directory traversal.'''

  def __init__(self, name, type, object, checkpoint):
    self.name = name
    self.type = type
    self.object = object
    self.checkpoint = checkpoint


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
      self._operation_names = self._send('getOperationNames').names
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

  def _post(self, endpoint, **kwargs):
    '''Sends an HTTP request to LynxKite and returns the response when it arrives.'''
    max_tries = 3
    for i in range(max_tries):
      r = self._get_session().post(
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

  def _request(self, endpoint, payload={}):
    '''Sends an HTTP JSON request to LynxKite and returns the response when it arrives.'''
    data = json.dumps(payload)
    return self._post(endpoint, data=data, headers={'Content-Type': 'application/json'})

  def _send(self, command, payload={}, raw=False):
    '''Sends a command to LynxKite and returns the response when it arrives.'''
    data = self._request('/remote/' + command, payload).text
    if raw:
      r = json.loads(data)
    else:
      r = json.loads(data, object_hook=_asobject)
    return r

  def sql(self, query, **mapping):
    '''Runs global level SQL query and returns a :class:`View` for the results.

    ``mapping`` maps :class:`Project`, :class:`Table`, or :class:`View` objects to names in your
    query. For example::

        my_view = lynx.sql('select * from `t`', t=my_table)
        result = lynx.sql('select * from `v`', v=my_view)
        result.export_csv('out.csv')
    '''
    checkpoints = {}
    for name, p in mapping.items():
      checkpoints[name] = p.checkpoint
    r = self._send('globalSQL', dict(
        query=query,
        checkpoints=checkpoints
    ))
    return View(self, r.checkpoint)

  def get_directory_entry(self, path):
    '''Returns details about a LynxKite path. The returned object has the following fields:
    ``exists``, ``isProject``, ``isTable``, ``isView``
    '''
    return self._send('getDirectoryEntry', dict(path=path))

  def get_prefixed_path(self, path):
    '''Resolves a path on a distributed file system. The path has to be specified using
    LynxKite's prefixed path syntax. (E.g. ``DATA$/my_file.csv``.)

    The returned object has an ``exists`` and a ``resolved`` attribute. ``resolved`` is a string
    containing the absolute path.
    '''
    return self._send('getPrefixedPath', dict(path=path))

  def import_csv(
          self,
          files,
          columnNames=[],
          delimiter=',',
          mode='FAILFAST',
          infer=True,
          columnsToImport=[]):
    '''Imports a CSV as a :class:`View`.'''
    return self._create_view(
        "CSV",
        dict(files=files,
             columnNames=columnNames,
             delimiter=delimiter,
             mode=mode,
             infer=infer,
             columnsToImport=columnsToImport))

  def import_hive(
          self,
          hiveTable,
          columnsToImport=[]):
    '''Imports a Hive table as a :class:`View`.'''
    return self._create_view(
        "Hive",
        dict(
            hiveTable=hiveTable,
            columnsToImport=columnsToImport))

  def import_jdbc(
          self,
          jdbcUrl,
          jdbcTable,
          keyColumn='',
          numPartitions=0,
          predicates=[],
          columnsToImport=[],
          properties={}):
    '''Imports a database table as a :class:`View` via JDBC.

    Args:
      jdbcUrl (str): The JDBC URL to connect to for this import task.
      jdbcTable (str): The name of the table to import in the source database.
      keyColumn (str, optional): The key column in the source table for Spark partitioning.
        The table should be partitioned or indexed for this column in the source database table
        for efficiency. Cannot be specified together with ``predicates``.
      numPartitions (int, optional): The number of Spark partitions to create.
      predicates (list of str, optional): List of SparkSQL where clauses to be executed on the
        source table for Spark partitioning. The table should be partitioned or indexed for this
        column in the source database table for efficiency. Cannot be specified together with
        ``keyColumn``.
      columnsToImport (list of str, optional): List of columns to import from the source table.
      properties (dict, optional): Extra settings for the JDBC connection. See the Spark
        documentation.
    '''
    return self._create_view(
        "Jdbc",
        dict(jdbcUrl=jdbcUrl,
             jdbcTable=jdbcTable,
             keyColumn=keyColumn,
             numPartitions=numPartitions,
             predicates=predicates,
             columnsToImport=columnsToImport,
             properties=properties))

  def import_parquet(
          self,
          files,
          columnsToImport=[]):
    '''Imports a Parquet file as a :class:`View`.'''
    return self._create_view(
        "Parquet",
        dict(columnsToImport=columnsToImport, files=files))

  def import_orc(
          self,
          files,
          columnsToImport=[]):
    '''Imports a Parquet file as a :class:`View`.'''
    return self._create_view(
        "ORC",
        dict(columnsToImport=columnsToImport, files=files))

  def import_json(
          self,
          files,
          columnsToImport=[]):
    '''Imports a JSON file as a :class:`View`.'''
    return self._create_view(
        "Json",
        dict(columnsToImport=columnsToImport, files=files))

  def get_parquet_metadata(self, path):
    '''Reads the metadata of a parquet file and returns the number of rows.'''
    r = self._send('getParquetMetadata', dict(path=path))
    return r

  def _create_view(self, format, dict):
    # TODO: remove this once #3859 is resolved.
    # These are required (as present in the case class), but are actually not read by the API
    # implementation. :(
    dict['table'] = ''
    dict['privacy'] = ''
    dict['overwrite'] = False
    res = self._send('createView' + format, dict)
    return View(self, res.checkpoint)

  def load_project(self, name):
    '''Loads an existing LynxKite project. Returns a :class:`RootProject`.'''
    r = self._send('loadProject', dict(name=name))
    project_checkpoint = _ProjectCheckpoint(self, r.checkpoint)
    return RootProject(project_checkpoint)

  def load_table(self, name):
    '''Loads an existing LynxKite table. Returns a :class:`Table`.'''
    r = self._send('loadTable', dict(name=name))
    return Table(self, r.checkpoint)

  def load_view(self, name):
    '''Loads an existing LynxKite view. Returns a :class:`View`.'''
    r = self._send('loadView', dict(name=name))
    return View(self, r.checkpoint)

  def new_project(self):
    '''Creates a new unnamed empty LynxKite :class:`RootProject`.'''
    r = self._send('newProject')
    project_checkpoint = _ProjectCheckpoint(self, r.checkpoint)
    return RootProject(project_checkpoint)

  def remove_name(self, name):
    '''Removes an object named ``name``.'''
    self._send('removeName', dict(name=name))

  def change_acl(self, file, readACL, writeACL):
    '''Sets the read and write access control list for a path (directory, project, etc) in LynxKite.
    '''
    self._send("changeACL",
               dict(project=file, readACL=readACL, writeACL=writeACL))

  def list_dir(self, dir=''):
    '''List the objects in a directory.
    Returns a list of :class:`DirectoryEntry` objects.'''

    object_lookup = {
        'project': lambda entry: RootProject(_ProjectCheckpoint(self, entry.checkpoint)),
        'view': lambda entry: View(self, entry.checkpoint),
        'table': lambda entry: Table(self, entry.checkpoint),
        'directory': lambda entry: Directory(self, entry.name),
    }

    result = self._send('list', dict(path=dir)).entries
    return [DirectoryEntry(
        name=entry.name,
        type=entry.objectType,
        checkpoint=entry.checkpoint,
        object=object_lookup[entry.objectType](entry)
    ) for entry in result]

  def upload(self, data, name=None):
    '''Uploads a file that can then be used in import methods.

    Use it to upload small test datasets from local files::

      with open('myfile.csv') as f:
        view = lk.import_csv(lk.upload(f))

    Or to upload even smaller datasets right from Python::

      view = lk.import_csv(lk.upload('id,name\\n1,Bob'))
    '''
    if name is None:
      name = 'remote-api-upload'  # A hash will be added anyway.
    return self._post('/ajax/upload', files=dict(file=(name, data))).text


class Table:

  def __init__(self, lynxkite, checkpoint):
    self.lk = lynxkite
    self.checkpoint = checkpoint
    self.name = '!checkpoint(%s,)|vertices' % checkpoint

  def save(self, name, writeACL=None, readACL=None):
    '''Saves the table under given name, with given writeACL and readACL.'''
    self.lk._send('saveTable', dict(
        checkpoint=self.checkpoint,
        name=name,
        writeACL=writeACL,
        readACL=readACL))

  def compute(self):
      '''Forces the computation of the table.'''
      fake_project = self.lk.new_project()
      fake_project.importVertices(**{
        'id-attr': '',
        'table': self.name})
      fake_project.compute()


class View:
  '''A LynxKite View is a definition of a data source.'''

  def __init__(self, lynxkite, checkpoint):
    self.lk = lynxkite
    self.checkpoint = checkpoint

  def save(self, name, writeACL=None, readACL=None):
    '''Saves the view under given name, with given writeACL and readACL.'''
    self.lk._send('saveView', dict(
        checkpoint=self.checkpoint,
        name=name,
        writeACL=writeACL,
        readACL=readACL))

  def take(self, limit):
    '''Computes the view and returns the result as a list. Only the first ``limit`` number of rows are returned.'''
    r = self.lk._send('takeFromView', dict(
        checkpoint=self.checkpoint,
        limit=limit,
    ), raw=True)
    return r['rows']

  def row_count(self):
    '''Returns the number of rows of the view.'''
    v = self.lk.sql('select count(*) as cnt from `v`', v=self).take(1)
    return v[0]['cnt']

  def schema(self):
    '''Computes the view and returns the schema.'''
    r = self.lk._send('getViewSchema', dict(
        checkpoint=self.checkpoint,
    ))
    return r

  def enforce_schema(self, schema_file):
    '''Raises an error if the schema of the view is different from the schema described in the specified file.
    If the file does not exist, the current schema is dumped into a new file,
    which later can be used as a reference schema'''
    schema = self.schema().to_dict()
    if os.path.isfile(schema_file):
      with open(schema_file, 'r') as f:
        required_schema = yaml.load(f)
        assert schema == required_schema, \
            'Incorrect schema: expected\n{}\nbut found\n{}'.format(required_schema, schema)
    else:
      os.makedirs(os.path.dirname(schema_file), exist_ok=True)
      with open(schema_file, 'w') as f:
        yaml.dump(schema, f, default_flow_style=False)

  def export_csv(self, path, header=True, delimiter=',', quote='"', shuffle_partitions=None):
    '''Exports the view to CSV file.

    ``shuffle_partitions``, if given, will determine the number of partitions in the output if Spark
    has to make a shuffle in the computation. Essentially,
    this sets the sql context parameter ``spark.sql.shuffle.partitions``

    '''
    self.lk._send('exportViewToCSV', dict(
        checkpoint=self.checkpoint,
        path=path,
        header=header,
        delimiter=delimiter,
        quote=quote,
        shufflePartitions=shuffle_partitions,
    ))

  def export_json(self, path, shuffle_partitions=None):
    '''Exports the view to JSON file.

    ``shuffle_partitions``, if given, will determine the number of partitions in the output if Spark
    has to make a shuffle in the computation. Essentially,
    this sets the sql context parameter ``spark.sql.shuffle.partitions``

    '''
    self.lk._send('exportViewToJson', dict(
        checkpoint=self.checkpoint,
        path=path,
        shufflePartitions=shuffle_partitions,
    ))

  def export_orc(self, path, shuffle_partitions=None):
    '''Exports the view to ORC file.

    ``shuffle_partitions``, if given, will determine the number of partitions in the output if Spark
    has to make a shuffle in the computation. Essentially,
    this sets the sql context parameter ``spark.sql.shuffle.partitions``

    '''
    self.lk._send('exportViewToORC', dict(
        checkpoint=self.checkpoint,
        path=path,
        shufflePartitions=shuffle_partitions,
    ))

  def export_parquet(self, path, shuffle_partitions=None):
    '''Exports the view to Parquet file.

    ``shuffle_partitions``, if given, will determine the number of partitions in the output if Spark
    has to make a shuffle in the computation. Essentially,
    this sets the sql context parameter ``spark.sql.shuffle.partitions``

    '''
    self.lk._send('exportViewToParquet', dict(
        checkpoint=self.checkpoint,
        path=path,
        shufflePartitions=shuffle_partitions
    ))

  def export_jdbc(self, url, table, mode='error'):
    '''Exports the view into a database table via JDBC.

    The "mode" argument describes what happens if the table already exists. Valid values are
    "error", "overwrite", and "append".
    '''
    self.lk._send('exportViewToJdbc', dict(
        checkpoint=self.checkpoint,
        jdbcUrl=url,
        table=table,
        mode=mode,
    ))

  def to_table(self):
    '''Exports the view to a :class:`Table`.'''
    res = self.lk._send('exportViewToTable', dict(checkpoint=self.checkpoint))
    return Table(self.lk, res.checkpoint)


class _ProjectCheckpoint:
  '''Class for storing the mutable state of a project.

  The `checkpoint` field is a checkpoint of a root project. The :class:`RootProject` and :class:`SubProject`
  classes can access and modify the checkpoint of a root project through this class.
  '''

  def __init__(self, lynxkite, checkpoint):
    self.checkpoint = checkpoint
    self.lk = lynxkite

  def save(self, name, writeACL, readACL):
    '''Saves the project having this project checkpoint under given name, with given writeACL and readACL.'''
    self.lk._send(
        'saveProject',
        dict(
            checkpoint=self.checkpoint,
            name=name,
            writeACL=writeACL,
            readACL=readACL))

  def compute(self):
    '''Computes all scalars and attributes of the project.'''
    return self.lk._send(
        'computeProject', dict(checkpoint=self.checkpoint))

  def is_computed(self):
    '''Checks whether all the scalars, attributes and segmentations of the project are already computed.'''
    r = self.lk._send('isComputed', dict(
        checkpoint=self.checkpoint
    ))
    return r

  def sql(self, query):
    '''Runs SQL queries.'''
    return self.lk.sql(query, **{'': self})

  def run_operation(self, operation, parameters, path):
    '''Runs an operation on the project with the given parameters.'''
    r = self.lk._send(
        'runOperation',
        dict(
            checkpoint=self.checkpoint,
            path=path,
            operation=operation,
            parameters=parameters))
    self.checkpoint = r.checkpoint

  def scalar(self, scalar, path):
    '''Fetches the value of a scalar. Returns either a double or a string.'''
    r = self.lk._send(
        'getScalar',
        dict(
            checkpoint=self.checkpoint,
            path=path,
            scalar=scalar))
    if hasattr(r, 'double'):
      return r.double
    return r.string

  def histogram(self, path, attr, attr_type, numbuckets, sample_size, logarithmic):
    '''Returns a histogram of the given attribute.'''
    request = dict(
        checkpoint=self.checkpoint,
        path=path,
        attr=attr,
        numBuckets=numbuckets,
        sampleSize=sample_size,
        logarithmic=logarithmic)
    if attr_type == 'vertex':
      r = self.lk._send('getVertexHistogram', request)
    elif attr_type == 'edge':
      r = self.lk._send('getEdgeHistogram', request)
    else:
      raise ValueError('Unknown attribute type: {type}'.format(type=attr_type))
    return r

  def copy(self):
    '''Returns another independent instance of _ProjectCheckpoint for the same checkpoint.'''
    return _ProjectCheckpoint(self.lk, self.checkpoint)

  def _metadata(self, path):
    '''Returns project metadata.'''
    request = dict(checkpoint=self.checkpoint, path=path)
    r = self.lk._send('getMetadata', request)
    return _Metadata(r)

  def global_name(self):
    '''Global reference of the project.'''
    return '!checkpoint({},)'.format(self.checkpoint)


class SubProject:
  '''Represents a root project or a segmentation.

  Example usage::

      import lynx
      p = lynx.LynxKite().new_project()
      p.exampleGraph()
      p.connectedComponents(**{
        'directions': 'ignore directions',
        'name': 'connected_components'})
      s = p.segmentation('connected_components')
      print(s.scalar('vertex_count'))

  '''

  def __init__(self, project_checkpoint, path):
    self.project_checkpoint = project_checkpoint
    self.path = path
    self.lk = project_checkpoint.lk

  def __dir__(self):
    '''Create list of methods for tab-completion.'''
    lk_ops = self.lk.operation_names()
    return super().__dir__() + lk_ops

  def scalar(self, scalar):
    '''Fetches the value of a scalar. Returns either a double or a string.'''
    return self.project_checkpoint.scalar(scalar, self.path)

  def run_operation(self, operation, parameters):
    '''Runs an operation on the project with the given parameters.'''
    self.project_checkpoint.run_operation(operation, parameters, self.path)
    return self

  def segmentation(self, name):
    '''Creates a :class:`SubProject` representing a segmentation of this subproject with the given name.'''
    return SubProject(self.project_checkpoint, self.path + [name])

  def vertex_attribute(self, attr):
    '''Creates an :class:`Attribute` representing a vertex attribute with the given name.'''
    return Attribute(attr, 'vertex', self.project_checkpoint, self.path)

  def edge_attribute(self, attr):
    '''Creates an :class:`Attribute` representing an edge attribute with the given name.'''
    return Attribute(attr, 'edge', self.project_checkpoint, self.path)

  def _metadata(self):
    '''Returns project metadata.'''
    return self.project_checkpoint._metadata(self.path)

  def _get_complex_view(self, request):
    '''Returns a `FEGraphResponse` object for testing purposes.
    The request is converted to a `FEGraphRequest`. For example
    req = dict(
        vertexSets=[
            dict(
                vertexSetId=vs,
                sampleSmearEdgeBundleId=eb,
                mode='sampled',
                filters=[],
                centralVertexIds=centers,
                attrs=[],
                xBucketingAttributeId='',
                yBucketingAttributeId='',
                xNumBuckets=1,
                yNumBuckets=1,
                radius=1,
                xAxisOptions=dict(logarithmic=False),
                yAxisOptions=dict(logarithmic=False),
                sampleSize=50000,
                maxSize=10000
            )],
        edgeBundles=[
            dict(
                srcDiagramId='idx[0]',
                dstDiagramId='idx[0]',
                srcIdx=0,
                dstIdx=0,
                edgeBundleId=eb,
                filters=[],
                layout3D=False,
                relativeEdgeDensity=False,
                maxSize=10000,
                edgeWeightId='',
                attrs=[]
            )])
    '''
    r = self.lk._send('getComplexView', request)
    return r

  def __getattr__(self, attr):
    '''For any unknown names we return a function that tries to run an operation by that name.'''
    def f(**kwargs):
      params = {}
      for k, v in kwargs.items():
        params[k] = str(v)
      return self.run_operation(attr, params)
    return f

  def global_table_name(self, table):
    '''Returns a reference to a table within the project. Example usage::

      project2.importVertices(**{
        'id-attr': 'id',
        'table': project1.global_table_name('edges')})

    The same set of project tables are accessible as from SQL.
    '''
    table_path = [self.project_checkpoint.global_name()] + self.path + [table]
    return '|'.join(table_path)

  def vertices_table(self):
    '''Global reference to the ``vertices`` table. Equivalent to ``global_table_name('vertices')``.
    '''
    return self.global_table_name('vertices')

  def edges_table(self):
    '''Global reference to the ``edges`` table. Equivalent to ``global_table_name('edges')``.'''
    return self.global_table_name('edges')


class RootProject(SubProject):
  '''Represents a project.'''

  def __init__(self, project_checkpoint):
    super().__init__(project_checkpoint, [])
    self.lk = project_checkpoint.lk

  def sql(self, query):
    '''Runs SQL queries.'''
    return self.project_checkpoint.sql(query)

  def df(self, query):
    '''Runs SQL queries.'''
    import pandas
    d = self.sql(query).take(-1)
    return pandas.DataFrame(d)

  def save(self, name, writeACL=None, readACL=None):
    '''Saves the project under given name, with given writeACL and readACL.'''
    self.project_checkpoint.save(name, writeACL, readACL)

  def copy(self):
    '''Returns another reference to the same project state, that can evolve independently.'''
    return RootProject(self.project_checkpoint.copy())

  def compute(self):
    '''Computes all scalars and attributes of the project.'''
    return self.project_checkpoint.compute()

  def is_computed(self):
    '''Checks whether all the scalars, attributes and segmentations of the project are already computed.'''
    return self.project_checkpoint.is_computed()

  @property
  def checkpoint(self):
    return self.project_checkpoint.checkpoint

  def global_name(self):
    '''Global reference of the project.'''
    return self.project_checkpoint.global_name()


class _Metadata():
  '''Wrapper class for storing and accessing project metadata.'''

  def __init__(self, data):
    self.data = data

  def vertex_set_id(self):
    return self.data.vertexSet

  def edge_bundle_id(self):
    return self.data.edgeBundle

  def belongs_to_id(self, segmentation_name):
    return [s.belongsTo for s in self.data.segmentations if s.name == segmentation_name][0]

  def vertex_attribute_id(self, attr_name):
    return [a.id for a in self.data.vertexAttributes if a.title == attr_name][0]

  def edge_attribute_id(self, attr_name):
    return [a.id for a in self.data.edgeAttributes if a.title == attr_name][0]


class Attribute():
  '''Represents a vertex or an edge attribute.'''

  def __init__(self, name, attr_type, project_checkpoint, path):
    self.project_checkpoint = project_checkpoint
    self.name = name
    self.attr_type = attr_type
    self.path = path

  def histogram(self, numbuckets=10, sample_size=None, logarithmic=False):
    '''Returns a histogram of the attribute.

    Example of precise logarithmic histogram with 20 buckets::

        a = p.vertex_attribute('attr_name')
        h = a.histogram(numbuckets=20, logarithmic=True)
        print(h.labelType)
        print(h.labels)
        print(h.sizes)

    '''

    return self.project_checkpoint.histogram(
        self.path,
        self.name,
        self.attr_type,
        numbuckets,
        sample_size,
        logarithmic)


class Directory:
  '''Represents a LynxKite directory'''

  def __init__(self, lynxkite, path):
    self.path = path
    self.lk = lynxkite

  def list(self):
    '''Returns the contents of this directory.'''
    return self.lk.list_dir(self.path)


class LynxException(Exception):
  '''Raised when LynxKite indicates that an error has occured while processing a command.'''

  def __init__(self, error):
    super(LynxException, self).__init__(error)
    self.error = error


class ResponseObject(types.SimpleNamespace):

  @staticmethod
  def obj_to_dict(obj):
    if isinstance(obj, ResponseObject):
      return obj.to_dict()
    elif isinstance(obj, list):
      return [ResponseObject.obj_to_dict(o) for o in obj]
    elif isinstance(obj, dict):
      return {k: ResponseObject.obj_to_dict(v) for (k, v) in obj.items()}
    else:
      return obj

  def to_dict(self):
    return {k: ResponseObject.obj_to_dict(v) for (k, v) in self.__dict__.items()}


def _asobject(dic):
  '''Wraps the dict in a namespace for easier access. I.e. d["x"] becomes d.x.'''
  return ResponseObject(**dic)


class PizzaKite(LynxKite):

  def __init__(self):
    super().__init__(address='https://pizzakite.lynxanalytics.com/')
    assert self.oauth_token(), 'Please set LYNXKITE_OAUTH_TOKEN.'
