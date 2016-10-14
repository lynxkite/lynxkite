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
      'interval-size': '0.001',
      'overlap': 'no'})
    seg1 = p.segmentation('seg1')
    seg1.addRandomVertexAttribute(**{
      'name': 'rnd2',
      'dist': 'Standard Uniform',
      'seed': '1234321'})
    seg1.segmentByDoubleAttribute(**{
      'name': 'seg2',
      'attr': 'rnd2',
      'interval-size': '0.001',
      'overlap': 'no'})
    seg2 = seg1.segmentation('seg2')
    self.assertEqual(seg2.path, ['seg1', 'seg2'])

  def test_get_segmentation_scalar(self):
    p = lynx.LynxKite().new_project()
    p.newVertexSet(size=100)
    p.addRandomVertexAttribute(**{
      'name': 'rnd1',
      'dist': 'Standard Uniform',
      'seed': '12343'})
    p.segmentByDoubleAttribute(**{
      'name': 'seg1',
      'attr': 'rnd1',
      'interval-size': '0.001',
      'overlap': 'no'})
    p.deriveScalar(**{
      'expr': '123',
      'output': 'derived-scalar',
      'type': 'double'})
    seg1 = p.segmentation('seg1')
    seg1.deriveScalar(**{
      'expr': '42',
      'output': 'derived-scalar',
      'type': 'double'})
    num = seg1.scalar('derived-scalar')
    self.assertEqual(num, 42.0)

if __name__ == '__main__':
  unittest.main()
