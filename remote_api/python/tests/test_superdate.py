import datetime
from lynx.luigi import core
import unittest


class TestSuperDate(unittest.TestCase):
  sd = core.SuperDate(datetime.datetime(2016, 10, 6, 18, 0, 0))

  def assertDateEquals(self, format_str, expected):
    return self.assertEqual(format_str.format(date=self.sd), expected)

  def test_superdate(self):
    self.assertDateEquals('{date}', '2016-10-06')
    self.assertDateEquals('{date.yesterday}', '2016-10-05')
    self.assertDateEquals('{date.minus8days}', '2016-09-28')
    self.assertDateEquals('{date.plus1year.minus1month}', '2017-09-06')
    self.assertDateEquals('{date.plus1year:%m/%d/%y}', '10/06/17')
    self.assertDateEquals(
        '{date.yesterday.oracle_timestamp}',
        "to_timestamp('2016-10-05 00:00:00', 'YYYY-MM-DD HH24:MI:SS')")
    self.assertDateEquals(
        '{date.yesterday.teradata_timestamp}',
        "CAST ('2016-10-05' AS TIMESTAMP(0) FORMAT 'YYYY-MM-DD')")
    self.assertDateEquals(
        '{date.yesterday.db2_timestamp}',
        "TIMESTAMP('2016-10-05 00:00:00')")

  def test_complex(self):
    self.assertDateEquals('''
    WHERE AR_ID > 0
    AND (
      LAST_UDT_TMS >= {date.yesterday.db2_timestamp}
        AND
      LAST_UDT_TMS < {date.db2_timestamp}
    ) AND (
      (CALL_YR = '{date.yesterday:%Y}' AND CALL_MTH = '{date.yesterday:%m}')
        OR
      (CALL_YR = '{date.yesterday.minus1week:%Y}' AND CALL_MTH = '{date.yesterday.minus1week:%m}')
    )
    WITH UR"
    ''', '''
    WHERE AR_ID > 0
    AND (
      LAST_UDT_TMS >= TIMESTAMP('2016-10-05 00:00:00')
        AND
      LAST_UDT_TMS < TIMESTAMP('2016-10-06 00:00:00')
    ) AND (
      (CALL_YR = '2016' AND CALL_MTH = '10')
        OR
      (CALL_YR = '2016' AND CALL_MTH = '09')
    )
    WITH UR"
    ''')


if __name__ == '__main__':
  unittest.main()
