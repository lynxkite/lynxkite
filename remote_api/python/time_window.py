import datetime
import dateutil
from dateutil import parser
import luigi
import lynx


class TimeWindowTarget(luigi.target.Target):
  
  def __init__(self, earliest, latest):
    self.earliest_start_time = earliest
    self.latest_start_time = latest

  def exists(self):
    now = datetime.datetime.now().time()
    return (self.earliest_start_time < now and now < self.latest_start_time)


class TimeWindowTask(luigi.task.ExternalTask):
  
  earliest_start_time = luigi.Parameter()
  latest_start_time = luigi.Parameter()

  def output(self):
    return TimeWindowTarget(dateutil.parser.parse(self.earliest_start_time).time(), dateutil.parser.parse(self.latest_start_time).time())


class TestTask(luigi.task.Task):

  def requires(self):
    return [TimeWindowTask('15:35', '23:00')]

  def output(self):
    return luigi.LocalTarget('testoutput.txt')

  def run(self):
    f = open('testoutput.txt','w')
    f.write('just testing')
    f.close()


