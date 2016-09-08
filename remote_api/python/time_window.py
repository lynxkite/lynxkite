import datetime
import dateutil
from dateutil import parser
import luigi
import lynx.luigi
import lynx.util
import re
import subprocess
import types


def time_windowed_method(func):
  def wrapper(self):
    now = datetime.datetime.now().time()
    if (self.earliest_start_time() < now and now < self.latest_start_time()):
      func(self)
    else:
      print("Can't run.")
  return wrapper


class TimeWindowedTask(luigi.Task):

  def earliest_start_time(self):
    """Override this to return the earliest time of the day windowed methods can be started,
       as a datetime.time object"""
    #raise NotImplementedError
    return dateutil.parser.parse('18:35').time()

  def latest_start_time(self):
    """Override this to return the latest time of the day windowed methods can be started,
       as a datetime.time object"""
    #raise NotImplementedError
    return dateutil.parser.parse('23:00').time()
  
  def run(self):
    pass

  def __new__(self):
    obj = luigi.Task.__new__(self)
    self.run = time_windowed_method(self.run)
    return obj

class ThisIsANormalTask:

  def run(self):
    print("I'm running.")


class ActualTask(ThisIsANormalTask, TimeWindowedTask):
  pass


print('testing')
at = ActualTask()
at.run()
