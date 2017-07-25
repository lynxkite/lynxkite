import datetime
import unittest
import lynx
from lynx.luigi import time_window
from unittest import mock


class TestTimeWindow(unittest.TestCase):

  def test_window(self):
    twt = time_window.DailyWindowTarget('12:00:01', '12:02')

    with mock.patch.object(time_window.DailyWindowTarget, 'now',
                           return_value=datetime.datetime.strptime('11:59', '%H:%M').time()):
      self.assertFalse(twt.exists())

    with mock.patch.object(time_window.DailyWindowTarget, 'now',
                           return_value=datetime.datetime.strptime('12:00', '%H:%M').time()):
      self.assertFalse(twt.exists())

    with mock.patch.object(time_window.DailyWindowTarget, 'now',
                           return_value=datetime.datetime.strptime('12:01', '%H:%M').time()):
      self.assertTrue(twt.exists())

    with mock.patch.object(time_window.DailyWindowTarget, 'now',
                           return_value=datetime.datetime.strptime('12:01', '%H:%M').time()):
      self.assertTrue(twt.exists())

    with mock.patch.object(time_window.DailyWindowTarget, 'now',
                           return_value=datetime.datetime.strptime('12:02', '%H:%M').time()):
      self.assertTrue(twt.exists())

    with mock.patch.object(time_window.DailyWindowTarget, 'now',
                           return_value=datetime.datetime.strptime('12:03', '%H:%M').time()):
      self.assertFalse(twt.exists())

  def test_inverse_window(self):
    twt = time_window.DailyWindowTarget('23:00', '02:00')
    with mock.patch.object(time_window.DailyWindowTarget, 'now',
                           return_value=datetime.datetime.strptime('22:59', '%H:%M').time()):
      self.assertFalse(twt.exists())

    with mock.patch.object(time_window.DailyWindowTarget, 'now',
                           return_value=datetime.datetime.strptime('23:00', '%H:%M').time()):
      self.assertTrue(twt.exists())

    with mock.patch.object(time_window.DailyWindowTarget, 'now',
                           return_value=datetime.datetime.strptime('23:01', '%H:%M').time()):
      self.assertTrue(twt.exists())

    with mock.patch.object(time_window.DailyWindowTarget, 'now',
                           return_value=datetime.datetime.strptime('02:00', '%H:%M').time()):
      self.assertTrue(twt.exists())

    with mock.patch.object(time_window.DailyWindowTarget, 'now',
                           return_value=datetime.datetime.strptime('02:01', '%H:%M').time()):
      self.assertFalse(twt.exists())


if __name__ == '__main__':
  unittest.main()
