"""Test data generator for an "Internet Of Things" scenario.

Usage:
  python iot_example.py vs > iot-example-vertices.csv
  python iot_example.py es > iot-example-edges.csv
"""
import collections
import math
import random
import sys

random.seed(0)


class Histogram(object):

  def __init__(self, x_min, x_max, *lines):
    """Generates random integers according to a distribution.

    Args:
      x_min: Lower bound. (Inclusive.)
      x_max: Upper bound. (Exclusive.)
      lines: A graphical representation of the probability distribution.
    """
    self.lines = lines
    self.line_length = max(len(l) for l in lines)
    self.x_max = float(x_max)
    self.x_min = float(x_min)

  def get(self):
    """Returns one random integer according to the configured distribution."""
    while True:
      x = random.uniform(self.x_min, self.x_max)
      i = int((x - self.x_min) * self.line_length / (self.x_max - self.x_min))
      i = min(i, self.line_length - 1)
      line = random.choice(self.lines)
      if i < len(line) and line[i] != ' ':
        return int(x)

age_hist = Histogram(0, 100,
                     '   ####',
                     '  #########     ##',
                     '  ####################',
                     ' ##########################',
                     '########################################')

young_income_hist = Histogram(10000, 100000,
                              '   ####',
                              '  #########     ##',
                              '  ####################',
                              ' ##########################',
                              '########################################')

old_income_hist = Histogram(0, 1000000,
                            '   ####',
                            '  #########     ##',
                            '  ####################',
                            ' ##########################',
                            '########################################')

device_hist = Histogram(0, 4,
                        '   ####',
                        '  #########     ##',
                        '  ####################',
                        ' ##########################',
                        '########################################')

Line = collections.namedtuple('Line', 'id, age, income, device')

vs = []
for i in range(10000):
  age = age_hist.get()
  if age < 30:
    income = young_income_hist.get()
  else:
    income = old_income_hist.get()
  device = ['laptop', 'phone', 'tablet', 'watch'][device_hist.get()]
  vs.append(Line(i, age, income, device))

if sys.argv[1] == 'vs':
  for v in vs:
    print ','.join(str(x) for x in v)

if sys.argv[1] == 'es':
  for i in range(1, 10000):
    for j in range(int(math.log(i, 1.01))):
      if vs[i].device == vs[j].device:
        print '{},{}'.format(i, j)
      elif random.random() < 0.2:
        print '{},{}'.format(i, j)
