import datetime
from lynx.luigi import core
import unittest


class TestSuperDate(unittest.TestCase):
  sd = core.SuperDate(datetime.datetime(2016, 10, 6, 18, 0, 0))

  def assertDateEquals(self, format_str, expected):
    return self.assertEquals(format_str.format(date=self.sd), expected)

  def test_superdate(self):
    self.assertDateEquals('{date}', '2016-10-06')
    self.assertDateEquals('{date.yesterday}', '2016-10-05')
    self.assertDateEquals('{date.minus8days}', '2016-09-28')
    self.assertDateEquals('{date.plus1year.minus1month}', '2017-09-06')
    self.assertDateEquals('{date.plus1year:%m/%d/%y}', '10/06/17')
    self.assertDateEquals(
        '{date.yesterday.oracle}',
        "to_timestamp('2016-10-05 00:00:00', 'YYYY-MM-DD HH24:MI:SS')")
    self.assertDateEquals(
        '{date.yesterday.teradata}',
        "CAST ('2016-10-05' AS TIMESTAMP(0) FORMAT 'YYYY-MM-DD')")
    self.assertDateEquals(
        '{date.yesterday.db2}',
        "TIMESTAMP('2016-10-05 00:00:00')")


if __name__ == '__main__':
  unittest.main()
