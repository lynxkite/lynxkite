import datetime
import dateutil
from dateutil import parser
import luigi
import lynx.luigi
import lynx.util
import re
import subprocess
import types


class TimeWindowedTask:

  def earliest_start_time(self):
    """Override this to return the earliest time of the day windowed methods can be started,
       as a datetime.time object"""
    #raise NotImplementedError
    return dateutil.parser.parse('18:04').time()

  def latest_start_time(self):
    """Override this to return the latest time of the day windowed methods can be started,
       as a datetime.time object"""
    #raise NotImplementedError
    return dateutil.parser.parse('23:00').time()

  def time_windowed_method(self, func):
    def wrapper(self):
      now = datetime.datetime.now().time()
      if (self.earliest_start_time() < now and now < self.latest_start_time()):
        func()
      else:
        print("Can't run.")
    return wrapper(self)

  #def __new__(self):
   # run = time_windowed_method(getattr(self, 'run'))



class TestClass:

  def run(self):
    print("I'm running.")


class TestClass2(TestClass, TimeWindowedTask):
  
  def whatever(self):
    return True


print('testing')
mtc = TestClass2()
mtc.run = mtc.time_windowed_method(mtc.run)
mtc.run()
