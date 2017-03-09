import unittest
import lynx


class TestSegmentations(unittest.TestCase):

  def test_segmentation_name(self):
    p = lynx.LynxKite().new_project()
    p.newVertexSet(size=30)
    p.addRandomVertexAttribute(**{
        'name': 'rnd1',
        'dist': 'Standard Uniform',
        'seed': '1234321'})
    p.segmentByDoubleAttribute(**{
        'name': 'seg1',
        'attr': 'rnd1',
        'interval_size': '0.001',
        'overlap': 'no'})
    seg1 = p.segmentation('seg1')
    seg1.addRandomVertexAttribute(**{
        'name': 'rnd2',
        'dist': 'Standard Uniform',
        'seed': '1234321'})
    seg1.segmentByDoubleAttribute(**{
        'name': 'seg2',
        'attr': 'rnd2',
        'interval_size': '0.001',
        'overlap': 'no'})
    seg2 = seg1.segmentation('seg2')
    self.assertEqual(seg2.path, ['seg1', 'seg2'])

  def test_get_segmentation_scalar(self):
    p = lynx.LynxKite().new_project()
    p.exampleGraph()
    p.connectedComponents(**{
        'directions': 'ignore directions',
        'name': 'connected_components'})
    s = p.segmentation('connected_components')
    num = s.scalar('vertex_count')
    self.assertEqual(num, 2.0)

  def test_segmentation_operations(self):
    lk = lynx.LynxKite()
    p = lk.new_project()
    p.newVertexSet(size=30)
    p.createRandomEdgeBundle(**{
        'degree': '1',
        'seed': '112233'})
    p.connectedComponents(**{
        'directions': 'ignore directions',
        'name': 'connected_components'})
    s = p.segmentation('connected_components')
    s.addRankAttribute(**{
        'keyattr': 'size',
        'order': 'ascending',
        'rankattr': 'ranking'})
    s.segmentByDoubleAttribute(**{
        'attr': 'ranking',
        'interval_size': '2',
        'name': 'bucketing',
        'overlap': 'no'})
    s2 = s.segmentation('bucketing')
    num = s2.scalar('vertex_count')
    self.assertEqual(num, 4.0)


if __name__ == '__main__':
  unittest.main()
