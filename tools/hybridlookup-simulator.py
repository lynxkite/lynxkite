"""A simple tool to demonstrate the distribution of the size data in the partitions
during hybrid lookup assuming a power law distribution of the sizes of the keys.
"""

import random
import math

# http://mathworld.wolfram.com/RandomNumber.html


def powerLaw(e):
  y = random.random()
  return math.pow(y, 1.0 / (e + 1.0))

NUM_KEYS = 1000000      # Total number of distinct keys.
PARTITION_SIZE = 20000  # KITE_VERTICES_PER_PARTITION.
THRESHOLD = 4000        # KITE_HYBRID_LOOKUP_THRESHOLD.
EXPONENT = 2            # The power law generator's exponent [2, inf).

total_records = 0
below_threshold = []
above_threshold = []

for x in range(0, NUM_KEYS):
  # Generate key sizes with a power law distribution.
  key_size = math.ceil(powerLaw(-EXPONENT))
  total_records += key_size
  if key_size < THRESHOLD:
    below_threshold.append(key_size)
  else:
    above_threshold.append(key_size)

# How many partitions would we have on the full dataset.
PARTITION_NUM = int(math.ceil(total_records / PARTITION_SIZE))
partition_load = [0] * PARTITION_NUM

for x in below_threshold:
  # We use the partitioner of the full dataset.
  p = random.randint(0, PARTITION_NUM - 1)
  partition_load[p] = partition_load[p] + x

print "record stats"
print "total records:", total_records
print "avg key size:", total_records / NUM_KEYS
print "small keys:", len(below_threshold)
print "large keys:", len(above_threshold), "of max:", math.ceil(total_records / THRESHOLD)

print "partition stats"
print "number of partitions:", PARTITION_NUM
print "min size:", min(partition_load)
# Avg is less then PARTITION_SIZE if we had large keys.
print "avg size:", sum(partition_load) / len(partition_load)
print "max size:", max(partition_load)
