"""Converts a SQLite-format tags file into a JSON-format tags file."""
import json
import sqlite3
import sys


def main():
  if len(sys.argv) != 3:
    print 'Usage:'
    print '  python sqlite2json.py <sqlite file> <json file>'
    sys.exit(1)

  f_in, f_out = sys.argv[1:]
  print 'converting', f_in, 'to', f_out, '...',
  write_json(f_out, read_sqlite(f_in))
  print 'done'


def read_sqlite(fn):
  with sqlite3.connect(fn) as conn:
    c = conn.cursor()
    c.execute('SELECT * FROM tags')
    return c.fetchall()


def write_json(fn, rows):
  obj = dict(rows)
  with file(fn, 'w') as f:
    json.dump(obj, f, indent=0)

if __name__ == '__main__':
  main()
