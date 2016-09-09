import datetime
import dateutil.parser
import luigi


class TimeWindowTarget(luigi.target.Target):

  def __init__(self, earliest, latest):
    self.earliest_start_time = dateutil.parser.parse(earliest).time()
    self.latest_start_time = dateutil.parser.parse(latest).time()

  def exists(self):
    now = datetime.datetime.now().time()
    return (self.earliest_start_time <= now and now <= self.latest_start_time)


class InTimeWindow(luigi.task.ExternalTask):

  earliest_start_time = luigi.Parameter()
  latest_start_time = luigi.Parameter()

  def output(self):
    return TimeWindowTarget(self.earliest_start_time, self.latest_start_time)


class TestTask(luigi.task.Task):

  def requires(self):
    return [InTimeWindow('18:35', '23:00')]

  def output(self):
    return luigi.LocalTarget('testoutput.txt')

  def run(self):
    print('just testing, actually creating the output is undesired')
