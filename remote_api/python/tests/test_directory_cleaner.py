import datetime
import unittest
from lynx.luigi.directory_cleaner import DirectoryCleaner
from unittest import mock
import types


class TestDirectoryCleaner(unittest.TestCase):

  @mock.patch('os.walk')
  @mock.patch('os.remove')
  @mock.patch('os.path.getctime')
  @mock.patch('shutil.rmtree')
  def test_run(self, rmtree, getctime, remove, walk):
    walk.return_value = [
        ('/tmp',
         ['old, no ttl', 'old, long ttl(ttl=7d)', 'old, short ttl (ttl=1m)', 'new, no ttl'],
            ['old file, no ttl']
         ),
        ('/tmp/old, no ttl', [], ['old, short ttl (ttl=1m)'])
    ]
    getctime.side_effect = lambda x: {
        '/tmp/old, no ttl': datetime.datetime(2017, 4, 10, 15, 32).timestamp(),
        '/tmp/old, long ttl(ttl=7d)': datetime.datetime(2017, 4, 10, 15, 32).timestamp(),
        '/tmp/old, short ttl (ttl=1m)': datetime.datetime(2017, 4, 10, 15, 32).timestamp(),
        '/tmp/new, no ttl': datetime.datetime(2017, 4, 15, 15, 32).timestamp(),
        '/tmp/old file, no ttl': datetime.datetime(2017, 4, 10, 15, 32).timestamp(),
        '/tmp/old, no ttl/old, short ttl (ttl=1m)': datetime.datetime(2017, 4, 10, 15, 32).timestamp(),
    }[x]

    task = DirectoryCleaner(date=datetime.datetime(2017, 4, 15, 16, 00), directory='/tmp')
    output = mock.MagicMock()
    with mock.patch.object(task, 'output', return_value=mock.DEFAULT) as m:
      m.return_value = output
      task.run()

    self.assertEqual(1, rmtree.call_count)
    rmtree.assert_any_call('/tmp/old, short ttl (ttl=1m)')
    self.assertEqual(1, remove.call_count)
    remove.assert_any_call('/tmp/old, no ttl/old, short ttl (ttl=1m)')
    # Assert the marker file is created.
    output.open.assert_called_once_with('w')


if __name__ == '__main__':
  unittest.main()
