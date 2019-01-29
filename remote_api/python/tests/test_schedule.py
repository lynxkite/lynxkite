import unittest
import lynx.kite
import lynx.automation
from lynx.automation import _aware_to_iso_str, _assert_is_utc, _assert_is_aware
from lynx.automation import Schedule
from lynx.automation import UTC, utc_dt
from datetime import datetime, timezone
import pendulum
import contextlib


class TestSchedule(unittest.TestCase):

  @contextlib.contextmanager
  def _assertExceptionMsg(self, exception_msg):
    with self.assertRaises(AssertionError) as exc:
      yield
    error_msg = exc.exception.args[0]
    self.assertIn(exception_msg, error_msg)

  def test_is_aware(self):
    dt = datetime(2019, 1, 18)
    singapore = pendulum.timezone('Asia/Singapore')
    aware_dt = pendulum.create(2019, 1, 18, tz=singapore)
    with self._assertExceptionMsg('You have to use pendulum to create datetimes.'):
      _assert_is_aware(dt)
    _assert_is_aware(aware_dt)

  def test_unaware_dt_not_allowed(self):
    with self._assertExceptionMsg('You have to use pendulum to create datetimes.'):
      lynx.automation.Schedule(datetime(2019, 1, 16), '0 0 * * *')

  def test_aware_dt_is_allowed(self):
    dt = pendulum.create(2019, 1, 16, tz=pendulum.timezone('Asia/Singapore'))
    schedule = lynx.automation.Schedule(dt, '0 0 * * *')

  def test_to_iso_str(self):
    singapore = pendulum.timezone('Asia/Singapore')
    budapest = pendulum.timezone('Europe/Budapest')
    dt_singapore = pendulum.create(2019, 1, 16, tz=singapore)
    dt_budapest1 = pendulum.create(2018, 3, 25, 1, tz=budapest)
    dt_budapest2 = pendulum.create(2018, 3, 25, 4, tz=budapest)
    dt_utc = utc_dt(2019, 1, 16)
    dt_naive = datetime(2019, 1, 16)
    lisbon = pendulum.timezone('Europe/Lisbon')
    dt_lisbon = pendulum.create(2019, 1, 16, tz=lisbon)
    dt_lisbon_summer = pendulum.create(2019, 4, 16, tz=lisbon)
    self.assertEqual(_aware_to_iso_str(dt_singapore), '2019-01-16T00:00:00+08:00')
    self.assertEqual(_aware_to_iso_str(dt_budapest1), '2018-03-25T01:00:00+01:00')
    self.assertEqual(_aware_to_iso_str(dt_budapest2), '2018-03-25T04:00:00+02:00')
    self.assertEqual(_aware_to_iso_str(dt_utc), '2019-01-16T00:00:00+00:00')
    with self._assertExceptionMsg('You have to use pendulum to create datetimes.'):
      _aware_to_iso_str(dt_naive)
    self.assertEqual(_aware_to_iso_str(dt_lisbon), '2019-01-16T00:00:00+00:00')
    self.assertEqual(_aware_to_iso_str(dt_lisbon_summer), '2019-04-16T00:00:00+01:00')

  def test_is_utc(self):
    dt1 = utc_dt(2019, 1, 16)
    singapore = pendulum.timezone('Asia/Singapore')
    dt2 = pendulum.create(2019, 1, 16, tz=singapore)
    dt3 = datetime(2019, 1, 16)
    lisbon = pendulum.timezone('Europe/Lisbon')
    dt4 = pendulum.create(2019, 1, 16, tz=lisbon)
    dt5 = pendulum.create(2019, 4, 16, tz=lisbon)
    _assert_is_utc(dt1)
    with self._assertExceptionMsg('Timezone is not UTC'):
      _assert_is_utc(dt2)
    with self._assertExceptionMsg('You have to use pendulum to create datetimes.'):
      _assert_is_utc(dt3)
    with self._assertExceptionMsg('Timezone is not UTC'):
      _assert_is_utc(dt4)
    with self._assertExceptionMsg('Timezone is not UTC'):
      _assert_is_utc(dt5)

  def test_lisbon_vs_utc(self):
    lisbon = pendulum.timezone('Europe/Lisbon')
    start_lisbon = pendulum.create(2018, 3, 25, 0, tz=lisbon)
    start_utc = pendulum.create(2018, 3, 25, 0)
    schedule = Schedule(start_lisbon, '0 5 * * *')
    utc_start = pendulum.create(2018, 3, 24)
    utc_end = pendulum.create(2018, 3, 27)
    self.assertEqual(
        [_aware_to_iso_str(d) for d in schedule.utc_dates(utc_start, utc_end)],
        [
            '2018-03-24T05:00:00+00:00',
            '2018-03-25T04:00:00+00:00',
            '2018-03-26T04:00:00+00:00'])
    schedule = Schedule(start_utc, '0 5 * * *')
    self.assertEqual(
        [_aware_to_iso_str(d) for d in schedule.utc_dates(utc_start, utc_end)],
        [
            '2018-03-24T05:00:00+00:00',
            '2018-03-25T05:00:00+00:00',
            '2018-03-26T05:00:00+00:00'])

  def test_pendulum_convert_to_utc(self):
    utc_date = pendulum.create(2018, 5, 1, 0, 0)  # UTC by default
    self.assertEqual(_aware_to_iso_str(utc_date), '2018-05-01T00:00:00+00:00')
    singapore = pendulum.timezone('Asia/Singapore')
    singapore_date = pendulum.create(2018, 5, 1, 0, 0, tz=singapore)
    self.assertEqual(
        _aware_to_iso_str(UTC.convert(singapore_date)),
        '2018-04-30T16:00:00+00:00')
    budapest = pendulum.timezone('Europe/Budapest')
    budapest_date = pendulum.create(2018, 5, 1, 0, 0, tz=budapest)
    self.assertEqual(
        _aware_to_iso_str(UTC.convert(budapest_date)),
        '2018-04-30T22:00:00+00:00')
    budapest_winter_date = pendulum.create(2018, 3, 25, 1, 0, tz=budapest)
    self.assertEqual(
        _aware_to_iso_str(UTC.convert(budapest_winter_date)),
        '2018-03-25T00:00:00+00:00')
    budapest_summer_date = pendulum.create(2018, 3, 25, 4, 0, tz=budapest)
    self.assertEqual(
        _aware_to_iso_str(UTC.convert(budapest_summer_date)),
        '2018-03-25T02:00:00+00:00')

  def test_dt_is_valid(self):
    dt = utc_dt(2019, 1, 16)
    schedule = lynx.automation.Schedule(dt, '0 5 * * *')
    test_date = utc_dt(2019, 1, 20, 5, 0)
    schedule.assert_utc_dt_is_valid(utc_dt(2019, 1, 20, 5, 0))
    # Before start date
    with self._assertExceptionMsg('2019-01-15T05:00:00+00:00 preceeds start date = 2019-01-16T00:00:00+00:00'):
      schedule.assert_utc_dt_is_valid(utc_dt(2019, 1, 15, 5, 0))

    # Non-compatible with cron string
    with self._assertExceptionMsg('is not valid with this Schedule'):
      schedule.assert_utc_dt_is_valid(utc_dt(2019, 1, 21, 4, 0))

  def test_schedule_utc_dates(self):
    schedule = Schedule(utc_dt(2019, 1, 1, 2, 0), '0 2 * * *')
    from_date = utc_dt(2019, 1, 1)
    to_date = utc_dt(2019, 1, 5)
    self.assertEqual(
        [_aware_to_iso_str(d) for d in schedule.utc_dates(from_date, to_date)],
        ['2019-01-01T02:00:00+00:00',
            '2019-01-02T02:00:00+00:00',
            '2019-01-03T02:00:00+00:00',
            '2019-01-04T02:00:00+00:00'])
    singapore = pendulum.timezone('Asia/Singapore')
    schedule = Schedule(pendulum.create(2019, 1, 1, 2, 0, tz=singapore), '0 2 * * *')
    self.assertEqual(
        [_aware_to_iso_str(d) for d in schedule.utc_dates(from_date, to_date)],
        ['2019-01-01T18:00:00+00:00',
            '2019-01-02T18:00:00+00:00',
            '2019-01-03T18:00:00+00:00',
            '2019-01-04T18:00:00+00:00'])

  def test_step_back(self):
    schedule = Schedule(utc_dt(2019, 1, 1, 2, 0), '0 2 * * *')
    start = utc_dt(2019, 1, 20, 5, 0)
    self.assertEqual(_aware_to_iso_str(schedule.step_back(start, 1)), '2019-01-20T02:00:00+00:00')
    self.assertEqual(_aware_to_iso_str(schedule.step_back(start, 2)), '2019-01-19T02:00:00+00:00')
    self.assertEqual(_aware_to_iso_str(schedule.step_back(start, 21)), '2018-12-31T02:00:00+00:00')
    start = utc_dt(2019, 1, 20, 2, 0)
    self.assertEqual(_aware_to_iso_str(schedule.step_back(start, 1)), '2019-01-19T02:00:00+00:00')

  def test_next_date(self):
    schedule = Schedule(utc_dt(2019, 1, 1, 2, 0), '0 2 * * *')
    start = utc_dt(2019, 1, 20, 5, 0)
    self.assertEqual(_aware_to_iso_str(schedule.next_date(start)), '2019-01-21T02:00:00+00:00')
    budapest = pendulum.timezone('Europe/Budapest')
    schedule = Schedule(pendulum.create(2018, 3, 25, 2, 0, tz=budapest), '*/30 * * * *')
    budapest_start_date = pendulum.create(2018, 3, 25, 1, 30, tz=budapest)
    utc_start_date = UTC.convert(budapest_start_date)
    bp1 = schedule.next_date(utc_start_date)
    bp2 = schedule.next_date(bp1)
    bp3 = schedule.next_date(bp2)
    self.assertEqual(_aware_to_iso_str(bp1), '2018-03-25T01:00:00+00:00')
    self.assertEqual(_aware_to_iso_str(bp2), '2018-03-25T01:30:00+00:00')
    self.assertEqual(_aware_to_iso_str(bp3), '2018-03-25T02:00:00+00:00')

  def test_datetimes_compatible(self):
    schedule = Schedule(utc_dt(2019, 1, 1, 2, 0), '0 2 * * *')
    start = utc_dt(2019, 1, 20, 5, 0)
    end = utc_dt(2019, 1, 25, 5, 0)
    dates = schedule.utc_dates(start, end)
    # Generated dates can be used as start_date and end_date
    sub = schedule.utc_dates(dates[1], dates[-2])
    # Generated dates can be used in next_date
    next_date = schedule.next_date(dates[1])
    self.assertEqual(next_date, dates[2])
    # Generated dates can be used in step_back
    prev_date = schedule.step_back(dates[-2], 1)
    self.assertEqual(prev_date, dates[-3])
    # Generated dates can be used in utc_dt_is_valid
    schedule.assert_utc_dt_is_valid(dates[2])
