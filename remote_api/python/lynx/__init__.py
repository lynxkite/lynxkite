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
import http.cookiejar
import json
import os
import ssl
import sys
import types
import urllib

if sys.version_info.major < 3:
  raise Exception('At least Python version 3 is needed!')

default_sql_limit = 1000
default_privacy = 'public-read'


class LynxKite:
  '''A connection to a LynxKite instance.

  Some LynxKite API methods take a connection argument which can be used to communicate with
  multiple LynxKite instances from the same session. If no arguments to the constructor are
  provided, then a connection is created using the following environment variables:
  ``LYNXKITE_ADDRESS``, ``LYNXKITE_USERNAME``, ``LYNXKITE_PASSWORD``,
  ``LYNXKITE_PUBLIC_SSL_CERT``.
  '''

  def __init__(self, username=None, password=None, address=None, certfile=None):
    '''Creates a connection object.'''
    # Authentication and querying environment variables is deferred until the
    # first request.
    self._address = address
    self._username = username
    self._password = password
    self._certfile = certfile
    self.opener = self.build_opener()

  def build_opener(self):
    cj = http.cookiejar.CookieJar()
    cp = urllib.request.HTTPCookieProcessor(cj)
    if self.certfile():
      sslctx = ssl.create_default_context(
          ssl.Purpose.SERVER_AUTH,
          cafile=self.certfile())
      https = urllib.request.HTTPSHandler(context=sslctx)
      return urllib.request.build_opener(https, cp)
    else:
      return urllib.request.build_opener(cp)

  def address(self):
    return self._address or os.environ['LYNXKITE_ADDRESS']

  def username(self):
    return self._username or os.environ.get('LYNXKITE_USERNAME')

  def password(self):
    return self._password or os.environ.get('LYNXKITE_PASSWORD')

  def certfile(self):
    return self._certfile or os.environ.get('LYNXKITE_PUBLIC_SSL_CERT')

  def _login(self):
    self._request(
        '/passwordLogin',
        dict(
            username=self.username(),
            password=self.password(),
            method='lynxkite'))

  def _request(self, endpoint, payload={}):
    '''Sends an HTTP request to LynxKite and returns the response when it arrives.'''
    data = json.dumps(payload).encode('utf-8')
    req = urllib.request.Request(
        self.address().rstrip('/') + '/' + endpoint.lstrip('/'),
        data=data,
        headers={'Content-Type': 'application/json'})
    max_tries = 3
    for i in range(max_tries):
      try:
        with self.opener.open(req) as r:
          return r.read().decode('utf-8')
      except urllib.error.HTTPError as err:
        if err.code == 401 and i + 1 < max_tries:  # Unauthorized.
          self._login()
          # And then retry via the "for" loop.
        elif err.code == 500:  # Internal server error.
          raise LynxException(err.read())
        else:
          raise err

  def _send(self, command, payload={}, raw=False):
    '''Sends a command to LynxKite and returns the response when it arrives.'''
    data = self._request('/remote/' + command, payload)
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

  def schema(self):
    '''Computes the view and returns the schema.'''
    r = self.lk._send('getViewSchema', dict(
        checkpoint=self.checkpoint,
    ))
    return r

  def export_csv(self, path, header=True, delimiter=',', quote='"', shuffle_partitions=None):
    '''Exports the view to CSV file.'''
    self.lk._send('exportViewToCSV', dict(
        checkpoint=self.checkpoint,
        path=path,
        header=header,
        delimiter=delimiter,
        quote=quote,
    ), shufflePartitions=shuffle_partitions)

  def export_json(self, path, shuffle_partitions=None):
    '''Exports the view to JSON file. '''
    self.lk._send('exportViewToJson', dict(
        checkpoint=self.checkpoint,
        path=path,
    ), shufflePartitions=shuffle_partitions)

  def export_orc(self, path, shuffle_partitions=None):
    '''Exports the view to ORC file.'''
    self.lk._send('exportViewToORC', dict(
        checkpoint=self.checkpoint,
        path=path,
    ), shufflePartitions=shuffle_partitions)

  def export_parquet(self, path, shuffle_partitions=None):
    '''Exports the view to Parquet file.'''
    self.lk._send('exportViewToParquet', dict(
        checkpoint=self.checkpoint,
        path=path,
    ), shufflePartitions=shuffle_partitions)

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

  def _metadata(self, path):
    '''Returns project metadata.'''
    request = dict(checkpoint=self.checkpoint, path=path)
    r = self.lk._send('getMetadata', request)
    return _Metadata(r)


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


class RootProject(SubProject):
  '''Represents a project.'''

  def __init__(self, project_checkpoint):
    super().__init__(project_checkpoint, [])
    self.lk = project_checkpoint.lk

  def sql(self, query):
    '''Runs SQL queries.'''
    return self.project_checkpoint.sql(query)

  def save(self, name, writeACL=None, readACL=None):
    '''Saves the project under given name, with given writeACL and readACL.'''
    self.project_checkpoint.save(name, writeACL, readACL)

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
    return '!checkpoint(%s,)' % self.project_checkpoint.checkpoint


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
