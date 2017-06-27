import unittest
import lynx


class TestProjectMetadata(unittest.TestCase):

  def test_guids_segmentations_and_scalars(self):
    p = lynx.LynxKite().new_project()
    p.newVertexSet(size=123)
    md = p._metadata()
    self.assertNotEqual(md.vertex_set_id(), '')
    self.assertEqual(md.edge_bundle_id(), '')
    self.assertEqual(md.data.segmentations, [])
    self.assertEqual(md.data.scalars[0].title, '!vertex_count')

  def test_attributes(self):
    p = lynx.LynxKite().new_project()
    p.exampleGraph()
    md = p._metadata()
    self.assertEqual(len(md.data.edgeAttributes), 2)
    self.assertEqual(len(md.data.vertexAttributes), 6)
    titles = []
    for a in md.data.vertexAttributes:
      titles.append(a.title)
    self.assertEqual(titles, [
        'age',
        'gender',
        'id',
        'income',
        'location',
        'name'])
    titles = []
    for a in md.data.edgeAttributes:
      titles.append(a.title)
    self.assertEqual(titles, ['comment', 'weight'])


if __name__ == '__main__':
  unittest.main()
