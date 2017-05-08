'''Whatever we want to share between modules. This is not part of the public API.'''
import datetime
import re
import subprocess
import types


_DURATION_RE = re.compile(r'((?P<days>\d+)d)?((?P<hours>\d+)h)?((?P<minutes>\d+)m)?')


def parse_duration(s):
  '''Parses a duration string into a datetime object. Valid units are ``d``, ``h``, and ``m``.'''
  m = _DURATION_RE.fullmatch(s)
  assert m, 'Could not parse "{}" as a duration.'.format(s)
  units = {unit: int(amount) for unit, amount in m.groupdict().items() if amount}
  delta = datetime.timedelta(**units)
  assert delta != datetime.timedelta(0), 'Could not parse "{}" as a duration.'.format(s)
  return delta


class HDFS:
  '''HDFS utilities.'''

  @staticmethod
  def list(path, env=None, options=None):
    '''Returns a list of objects for the direct contents of the directory.

    Set ``env`` to customize environment variables, such as ``HADOOP_CONF_DIR``.
    '''
    if options:
      cmd = ['hadoop', 'fs', '-ls', options, path]
    else:
      cmd = ['hadoop', 'fs', '-ls', path]
    print(cmd)
    output = subprocess.check_output(cmd, env=env)
    return HDFS._parse_hadoop_ls(output)

  @staticmethod
  def _parse_hadoop_ls(output):
    '''Example output from hadoop fs -ls::

         Found 3 items
         -rw-r--r--   1 hadoop hadoop        393 2016-07-27 15:32 hdfs://ip-10-165-135-220:8020/user/hadoop/x/README.md
         -rw-r--r-- + 1 hadoop hadoop          0 2016-07-27 15:32 hdfs://ip-10-165-135-220:8020/user/hadoop/x/_SUCCESS
         -rw-r--r--   1 hadoop hadoop         16 2016-07-27 15:32 hdfs://ip-10-165-135-220:8020/user/hadoop/x/hello world

    The column widths are unpredictable. The second column contains either "+" or " ". (The ACL
    bit.) The file name may contain spaces. There is no other format available. We are parsing this.
    '''
    lines = output.decode('utf-8').strip().split('\n')
    if lines == ['']:
      return []
    # hadoop fs -ls -R does not print Found n items...
    if lines[0].startswith('Found '):
      lines = lines[1:]
    return [HDFS._parse_hadoop_ls_line(line) for line in lines]

  @staticmethod
  def _parse_hadoop_ls_line(line):
    pieces = line.split(None, 7)
    if pieces[1] == '+':  # ACL is not ' ' so the file name is from column 8.
      pieces = line.split(None, 8)
    else:  # ACL is ' ' which we insert back explicitly.
      pieces.insert(1, ' ')
    ns = types.SimpleNamespace()
    ns.permission, ns.acl, ns.replication, ns.owner, ns.group, ns.size, ns.date, ns.time, ns.path \
        = pieces
    ns.datetime = datetime.datetime.strptime(ns.date + ' ' + ns.time, '%Y-%m-%d %H:%M')
    return ns

  @staticmethod
  def rm(path, env=None):
    '''Recursively deletes a path.

    Set ``env`` to customize environment variables, such as ``HADOOP_CONF_DIR``.
    '''
    # ``hadoop fs -rm`` accepts globs, so if the filename contains characters used in globs we have
    # to escape those. This is absolutely necessary for ``[ttl=...]`` but is also useful for not
    # deleting a bunch of files in case a file name contains a ``*``.
    escaped = (
        path.replace('\\', '\\\\')
        .replace('[', '\\[').replace(']', '\\]')
        .replace('*', '\\*').replace('?', '\\?')
        .replace('{', '\\{').replace('}', '\\}'))
    cmd = ['hadoop', 'fs', '-rm', '-r', '-skipTrash', escaped]
    print(cmd)
    subprocess.check_call(cmd, env=env)
