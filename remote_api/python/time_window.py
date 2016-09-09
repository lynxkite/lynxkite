import datetime
import dateutil.parser
import luigi


class TimeWindowTarget(luigi.target.Target):
  """
  A target that represents a time interval.
  """

  def __init__(self, earliest, latest):
    self.earliest_start_time = dateutil.parser.parse(earliest).time()
    self.latest_start_time = dateutil.parser.parse(latest).time()

  def exists(self):
    now = datetime.datetime.now().time()
    return (self.earliest_start_time <= now and now <= self.latest_start_time)


class InTimeWindow(luigi.task.ExternalTask):
  """
  An external task, holding a TimeWindowTarget.
  Should you need to restrict a task to only run in a certain time interval,
  add an instance of this class to its 'requires' list.

  Usage example::

    class TestTask(luigi.task.Task):
      def requires(self):
        return [InTimeWindow('18:35', '23:00')]
  """

  earliest_start_time = luigi.Parameter()
  latest_start_time = luigi.Parameter()

  def output(self):
    return TimeWindowTarget(self.earliest_start_time, self.latest_start_time)
