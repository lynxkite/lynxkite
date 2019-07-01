import unittest
import lynx.kite
import os
import shutil


class TestDownload(unittest.TestCase):
  lk = lynx.kite.LynxKite()
  num_vertices = 200100  # We will have two partitions, assuming
  # KITE_VERTICES_PER_PARTITION is the default 200000
  rnd = os.urandom(16).hex()
  export_path = 'DATA$/' + str(rnd)

  def test_downloaded_parquet_is_healthy(self):
    a = self.lk.createVertices(size=self.num_vertices)
    b = a.sql('select * from `vertices`')
    path = self.lk.exportToParquetNow(b, path=self.export_path, for_download=True).run_export()
    c = self.lk.download_file(path)
    d = self.lk.uploadParquetNow(c)
    count = self.lk.useTableAsVertices(d).sql('select count(*) from `vertices`').df().iloc[0][0]
    self.assertEqual(self.num_vertices, int(count))

  @classmethod
  def tearDownClass(cls):
    resolved = cls.lk.get_prefixed_path(cls.export_path).resolved[len('file:'):]
    shutil.rmtree(resolved, ignore_errors=True)
