import contextlib
import configparser
import MySQLdb
import re


def parse_jdbc_connection_string(string):
  # mysql://<username>:<password>@<hostname>/<db>
  m = re.match('mysql://([^:]*):([^@]*)@([^/]*)/(.*)', string)
  return dict(username=m.group(1), password=m.group(2), hostname=m.group(3), db=m.group(4))


class NoMysqlConnectionSpecifiedException(Exception):
  pass


def create_mysql_connection(config_file):
  config = None
  if config_file is None:
    raise NoMysqlConnectionSpecifiedException()
  parser = configparser.ConfigParser()
  parser.read(config_file)
  if 'task_history' in parser and 'db_connection' in parser['task_history']:
    config = parse_jdbc_connection_string(parser['task_history']['db_connection'])
  if config is None:
    raise NoMysqlConnectionSpecifiedException()

  return contextlib.closing(MySQLdb.connect(
      host=config['hostname'],
      user=config['username'],
      passwd=config['password'],
      db=config['db']))


def commit_sql(conn, sql, **parameters):
  with contextlib.closing(conn.cursor()) as cursor:
    try:
      cursor.execute(sql, parameters)
      conn.commit()
    except BaseException:
      conn.rollback()
