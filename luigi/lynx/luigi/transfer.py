'''Classes for transferring data between various sources and destinations.'''
import luigi
import lynx.util
import os
import subprocess
import tempfile
import urllib
import yaml


class SCPTarget(luigi.Target):
  '''A directory that can be read/written through SCP.
  Requires passwordless SSH to be set up to the target host.
  '''

  def __init__(self, host, dirname, require_success=True):
    '''``host`` can include a username, such as ``root@host.example.com``. You just need to make
    sure that ``ssh <host>`` works without any prompts.

    Set ``require_success`` to ``False`` if the you want to read from this directory as soon as it
    exists. If set to ``True``, the target will be reported as existent only if
    ``<dirname>/_SUCCESS`` exists. (This is the better option, but ``require_success=False`` may be
    useful for externally controlled sources.)
    '''
    self.host = host
    self.dirname = dirname
    self.require_success = require_success

  def exists(self):
    if self.require_success:
      path = self.dirname + '/_SUCCESS'
    else:
      path = self.dirname
    cmd = ['ssh', self.host, "test -e '{}'".format(path)]
    print(cmd)
    error = subprocess.call(cmd)
    if error == 0:
      return True
    elif error == 1:
      return False
    else:
      raise Exception('Cannot tell if {}:{} exists.'.format(self.host, self.dirname))

  def human_name(self):
    return self.host + ':' + self.dirname

  def list_files(self):
    cmd = ['ssh', self.host, "ls -1 '{}'".format(self.dirname)]
    print(cmd)
    p = subprocess.check_output(cmd, stdout=subprocess.PIPE, universal_newlines=True)
    lst = p.stdout.strip()
    if not lst:
      return []
    else:
      return lst.split('\n')

  def read_file(self, fn):
    cmd = ['ssh', self.host, "cat '{}/{}'".format(self.dirname, fn)]
    print(cmd)
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE)
    return p.stdout

  def write_file(self, fn, stream):
    cmd = ['ssh', self.host, "cat > '{}/{}'".format(self.dirname, fn)]
    print(cmd)
    subprocess.check_call(cmd, stdin=stream)

  def create_directory(self):
    cmd = ['ssh', self.host, "mkdir -p '{}'".format(self.dirname)]
    print(cmd)
    subprocess.check_call(cmd)


class HDFSTarget(luigi.Target):
  '''A directory located on HDFS.'''

  def __init__(
          self, dirname,
          hadoop_conf_dir=None, kerberos_keytab=None, kerberos_principal=None,
          statter_address=None):
    '''Just provide ``dirname`` for a path that can be accessed with the local HDFS configuration.
    (E.g. if the local cluster is unsecure, any other unsecure cluster can be accessed without
    specifying the optional arguments.)

    For accessing clusters with different configurations, set the configuration directory with
    ``hadoop_conf_dir``.

    If you need to access a secure (Kerberos) HDFS cluster you can either run ``kinit`` yourself in
    the environment where the Luigi tasks run, or you can set ``kerberos_keytab`` and
    ``kerberos_principal``, so that the transfer operation can run ``kinit``. Since the credentials
    acquired by ``kinit`` have a short expiration, the keytab-based method is more suited to
    automated or long-running processes.

    A long-running Statter process may be used to speed up existence checks. If a
    ``statter_address`` (``hostname:port``) is provided, this Statter instance is consulted for
    existence checks. If ``statter_address`` is not set, the ``STATTER_ADDRESS`` environment
    variable is used if possible.
    '''
    self.dirname = dirname
    self.env = os.environ.copy()
    if hadoop_conf_dir:
      self.env['HADOOP_CONF_DIR'] = hadoop_conf_dir
    assert (kerberos_keytab is None) == (kerberos_principal is None), (
        'Please specify both kerberos_keytab and kerberos_principal. (Or neither.)')
    self.kerberos_keytab = kerberos_keytab
    self.kerberos_principal = kerberos_principal
    self.statter_address = statter_address or os.environ.get('STATTER_ADDRESS')

  def kerberos_kinit(self):
    if not self.kerberos_keytab:
      return
    # To make sure multiple authentications can be active at the same time, and that we use the
    # right one, we set KRB5CCNAME, the path where the credentials are stored. This is necessary,
    # for example, when transferring between two secure clusters.
    self.env['KRB5CCNAME'] = '/tmp/kerberos_' + self.kerberos_principal.replace('/', '_')
    cmd = ['kinit', '-kt', self.kerberos_keytab, self.kerberos_principal]
    print(cmd)
    subprocess.check_call(cmd, env=self.env)

  def exists(self):
    if self.statter_address:
      return statter(self.statter_address, self.dirname + '/_SUCCESS')
    else:
      self.kerberos_kinit()
      cmd = ['hadoop', 'fs', '-stat', self.dirname + '/_SUCCESS']
      print(cmd)
      returncode = subprocess.call(cmd, env=self.env)
      return returncode == 0

  def download_to(self, local_path):
    self.kerberos_kinit()
    cmd = ['hadoop', 'fs', '-cp', self.dirname + '/*', 'file:' + local_path]
    print(cmd)
    subprocess.check_call(cmd, env=self.env)

  def upload_from(self, local_path):
    self.kerberos_kinit()
    cmd = ['hadoop', 'fs', '-cp', 'file:' + local_path + '/*', self.dirname + '/']
    print(cmd)
    subprocess.check_call(cmd, env=self.env)

  def list_files(self):
    self.kerberos_kinit()
    files = lynx.util.HDFS.list(self.dirname, env=self.env)
    return [f.path.split('/')[-1] for f in files]

  def read_file(self, fn):
    self.kerberos_kinit()
    cmd = ['hadoop', 'fs', '-cat', self.dirname + '/' + fn]
    print(cmd)
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, env=self.env)
    return p.stdout

  def write_file(self, fn, stream):
    self.kerberos_kinit()
    cmd = ['hadoop', 'fs', '-put', '-', self.dirname + '/' + fn]
    print(cmd)
    subprocess.check_call(cmd, stdin=stream, env=self.env)

  def create_directory(self):
    self.kerberos_kinit()
    cmd = ['hadoop', 'fs', '-mkdir', '-p', self.dirname]
    print(cmd)
    subprocess.check_call(cmd, env=self.env)


class TransferTask(luigi.Task):
  '''Extend this task to create a directory transfer task.

  Override ``requires()`` and ``output()`` to specify the input and output.
  Both the input and output have to support the transfer API, like the target classes in this
  module.

  TransferTask defines a ``ttl`` attribute. Set it to a duration specified in hours, days, or
  minutes (for example ``48h``, ``7d``, ``1h30m``) in your subclass. The temporal local copy of the
  files transfered will be deleted by a periodic cleanup task (LocalDirectoryCleaner) once it
  becomes older than the set duration. If ``ttl`` is not set and the transfer task fails during the
  transfer stage, the dangling temporal files will never be automatically deleted.

  Override the ``transform_fn`` attribute with a function if you want to make some transformation
  on the transfered data. If overriden, ``transform_fn`` must accept a ``local_src`` and a
  ``local_dst`` argument and it is expected to transform entities under ``local_src`` into entities
  under ``local_dst``.

  Example for copying an externally produced directory from SCP to HDFS::

    class SCPtoHDFSDemo(lynx.luigi.TransferTask):
      def requires(self):
        return lynx.luigi.SCPExternalTask(
          'user@hostname', '/path/to/directory', require_success=False)
      def output(self):
        return lynx.luigi.HDFSTarget('hdfs://hostname:9000/destination_file_name')

  Example for copying the output of a :class:`LynxTableFileTask` to an SCP destination::

    class HDFStoSCPDemo(lynx.luigi.TransferTask):
      def requires(self):
        return MyLynxTableFileTask()
      def output(self):
        return lynx.luigi.SCPTarget('user@hostname', '/path/to/destination')

  For a target to support the transfer API as a *source*, it needs to implement the following
  methods:

  ``list_files()``
    Returns a list of the file names in this source. (Just local names, not full paths.)

  ``read_file(filename)``
    Returns a file object representing the stream of bytes from the file.

  ``download_to(local_path)`` (Optional.)
    Downloads all the files from the source into a local directory.

  For a target to support the transfer API as a *destination*, it needs to implement the following
  methods:

  ``create_directory()``
    Creates the destination directory.

  ``write_file(filename, stream)``
    Writes the bytes to the destination file from the provided file object.

  ``upload_from(local_path)`` (Optional.)
    Uploads all the files from a local directory to the destination directory.

  A target can also support the transfer API by providing in its place another target that supports
  the API:

  ``transfer_target()``
    Returns a target that supports the transfer API. This target will be used instead.
  '''

  ttl = '1d'
  transform_fn = None
  stream = False

  @staticmethod
  def _unwrap_target(target):
    while hasattr(target, 'transfer_target'):
      target = target.transfer_target()
    return target

  @staticmethod
  def _stream_src(src):
    names = (name for name in src.list_files() if name != '_SUCCESS')
    for name in names:
      yield name, src.read_file(name)

  def _download(self, src, local_path):
    if hasattr(src, 'download_to'):
      src.download_to(local_path)
    else:
      for name, stream in self._stream_src(src):
        with open(os.path.join(local_path, name), 'wb') as local_file:
          local_file.write(stream.read())

  @staticmethod
  def _upload(local_path, dst):
    dst.create_directory()
    if hasattr(dst, 'upload_from'):
      with open(os.path.join(local_path, '_SUCCESS'), 'w') as success:
        pass
      dst.upload_from(local_path)
    else:
      names = (name for name in os.listdir(local_path) if name != '_SUCCESS')
      for name in names:
        file_path = os.path.join(local_path, name)
        assert os.path.isfile(file_path), 'Can not upload content in subdirectory.'
        with open(file_path, 'rb') as stream:
          dst.write_file(name, stream)
      dst.write_file('_SUCCESS', subprocess.DEVNULL)

  def _transfer_by_streaming(self, src, dst):
    dst.create_directory()
    for name, stream in self._stream_src(src):
      dst.write_file(name, stream)
    dst.write_file('_SUCCESS', subprocess.DEVNULL)

  def _transfer_by_local_copy(self, src, dst):
    suffix = '(ttl={})'.format(self.ttl) if self.ttl else None
    with tempfile.TemporaryDirectory(suffix=suffix) as tmp:
      local_original = os.path.join(tmp, 'local_original')
      os.mkdir(local_original)
      self._download(src, local_original)
      if self.transform_fn:
        local_transformed = os.path.join(tmp, 'local_transformed')
        os.mkdir(local_transformed)
        self.transform_fn(local_original, local_transformed)
        path_to_upload = local_transformed
      else:
        path_to_upload = local_original
      self._upload(path_to_upload, dst)

  def run(self):
    src = self._unwrap_target(self.input())
    dst = self._unwrap_target(self.output())
    if self.stream:
      assert self.transform_fn is None, 'Can not transform streams'
      self._transfer_by_streaming(src, dst)
    else:
      self._transfer_by_local_copy(src, dst)


class SchemaEnforcedTransferTask(TransferTask):
  """
  A schema enforced transfer task.

  Raises an error if the schema of its ``input().view(lk)`` has been altered
  since the first run.

  Usage example::
    class MyTask(SchemaEnforcedTransferTask)
  """
  schema_directory = None

  def run(self):
    if self.schema_directory:
      schema_file = self.schema_directory + '/' + self.task_family + '.schema'
      inputview = self.input().view(self.input()._lk_for_exists)
      inputview.enforce_schema(schema_file)
    super().run()


class SCPExternalTask(luigi.ExternalTask):
  '''An external task that outputs an :class:`SCPTarget`.'''
  host = luigi.Parameter()
  dirname = luigi.Parameter()
  require_success = luigi.Parameter(default=True)

  def output(self):
    return SCPTarget(self.host, self.dirname, self.require_success)


def statter(address, filename):
  with urllib.request.urlopen('http://{}/stat/{}'.format(address, filename)) as response:
    return response.read() == b'true'
