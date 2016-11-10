import unittest
import lynx
import random
import string
import glob
import os
from os.path import expanduser
import shutil
import getpass

# I tested this with 4 parallel test processes, and the tests seem to
# run forever without failure. But implementing this parallelism into
# this automated test requires more effort than a have capacity for.


class TestParquetPartitioning(unittest.TestCase):

  def do_test_parquet_partitioning(self):
    path = ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(15))
    partitions = random.randint(190, 210)
    print ("test_parquest_partitioning: " + path + "  partitions: " + str(partitions))
    lk = lynx.LynxKite()
    p = lk.new_project()

    size = 1000
    p.newVertexSet(size=size)
    sql = 'SELECT ordinal from `p` GROUP BY ordinal'
    view = lk.sql(sql, p=p)

    data_path = "DATA$/" + path
    if partitions != 200:
      view.export_parquet(data_path, partitions)
    else
      view.export_parquet(data_path)
    # Check number of parquet files:
    resolved_path = lk.get_prefixed_path(data_path).resolved
    # Cut file: from the beginning
    raw_path = resolved_path[5:]
    files = os.listdir(raw_path)
    num_files = len(list(filter(lambda file: file.endswith('.gz.parquet'), files)))
    self.assertEqual(num_files, partitions)

    # Check data integrity
    view2 = lk.import_parquet(data_path)
    result = lk.sql('select SUM(ordinal) from `v`', v=view2)
    ordinal_sum = result.take(1)[0].get('_c0')
    self.assertEqual(ordinal_sum, size * (size - 1) / 2)

    # Clean up, if everything was okay
    shutil.rmtree(raw_path)

  def test_parquest_partitioning(self):
    while True:
      self.do_test_parquet_partitioning()
      # Saving Jenkins from this loop
      if getpass.getuser() != 'gabor':
        break


if __name__ == '__main__':
  unittest.main()
