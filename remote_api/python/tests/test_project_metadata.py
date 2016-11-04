import unittest
import lynx


class TestProjectMetadata(unittest.TestCase):

  def test_guids_segmentations_and_scalars(self):
    p = lynx.LynxKite().new_project()
    p.newVertexSet(size=123)
    md = p.meta_data()
    self.assertNotEqual(md.vertexSet, '')
    self.assertEqual(md.edgeBundle, '')
    self.assertEqual(md.segmentations, [])
    self.assertEqual(md.scalars[0].title, 'vertex_count')

  def test_attributes(self):
    p = lynx.LynxKite().new_project()
    p.exampleGraph()
    md = p.meta_data()
    self.assertEqual(len(md.edgeAttributes), 2)
    self.assertEqual(len(md.vertexAttributes), 6)
    titles = []
    for a in md.vertexAttributes:
      titles.append(a.title)
    self.assertEqual(titles, [
        'age',
        'gender',
        'id',
        'income',
        'location',
        'name'])
    titles = []
    for a in md.edgeAttributes:
      titles.append(a.title)
    self.assertEqual(titles, ['comment', 'weight'])

  def test_centers(self):
    p = lynx.LynxKite().new_project()
    p.newVertexSet(size=123)
    c = p.centers(4, p.meta_data().vertexSet)
    self.assertEqual(c,[
      '-8730854457551617996',
      '-8599056917853110189',
      '-8372471039069781959',
      '-8361175665923325924'])

if __name__ == '__main__':
  unittest.main()
