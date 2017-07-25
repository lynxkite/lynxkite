"""
Represents a daily time window.

Can be used to restrict task start times to a certain time interval.
"""

import datetime
import dateutil.parser
import luigi


class DailyWindowTarget(luigi.target.Target):
  """
  A target that represents a daily time interval.
  For further explanation see class InDailyWindow.
  """

  def __init__(self, earliest, latest):
    self.earliest_start_time = dateutil.parser.parse(earliest).time()
    self.latest_start_time = dateutil.parser.parse(latest).time()

  def now(self):
    return datetime.datetime.now().time()

  def exists(self):
    now = self.now()
    if self.earliest_start_time <= self.latest_start_time:
      # for intervals during the day, e.g. 18:35 - 23:00
      return self.earliest_start_time <= now <= self.latest_start_time
    else:
      # for intervals carrying over to the next day, e.g. 23:00 - 02:00
      return not self.latest_start_time < now < self.earliest_start_time


class InDailyWindow(luigi.task.ExternalTask):
  """
  An external task, holding a DailyWindowTarget.
  Should you need to restrict a task to only run in a certain
  daily time interval, simply add an instance of this class to its 'requires' list.

  Usage example::

    class TestTask(luigi.task.Task):
      def requires(self):
        return [InDailyWindow('18:35', '23:00')]
        # can be started only between 18:35 and 23:00

    class TestTask2(luigi.task.Task):
      def requires(self):
        return [InDailyWindow('23:00', '02:00')]
        # can be started only between 23:00 and 02:00

  Pay special attention to the time format. Time strings are parsed with dateutil.parser,
  what provides great flexibility with all its advantages and disadvantages.

  For example, all of the following will be parsed correctly:
  '12:01', '12:01:13', '12:01:13.44', '15:36', '15h36m', '3:36 pm', 'T15h'.

  All the following strings will be parsed too, without any errors,
  but with potentially surprising results:
    | '1200' - interpreted as year
    | 'T1500' - interpreted as year
    | 'T15' - interpreted as day
    | '12:01.23' - interpreted as fractional minute, .23 = floor(60*23/100) = 13 seconds

  For detailed explanation see the dateutil module documentation.
  Although dateutil is capable of handling dates,
  this class is just a daily time window, thus date information (including timezone)
  will be ignored, only time is kept.
  """

  earliest_start_time = luigi.Parameter()
  latest_start_time = luigi.Parameter()

  def output(self):
    return DailyWindowTarget(self.earliest_start_time, self.latest_start_time)
