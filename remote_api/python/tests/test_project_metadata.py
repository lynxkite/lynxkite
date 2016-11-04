import unittest
import lynx


class TestProjectMetadata(unittest.TestCase):

  def test_guids(self):
    p = lynx.LynxKite().new_project()
    p.newVertexSet(size=123)
    p.addRandomVertexAttribute(**{
        'name': 'attribute_name',
        'dist': 'Standard Uniform',
        'seed': '12344321'})
    a = p.vertex_attribute('attribute_name')
    h = a.histogram(numbuckets=10, logarithmic=True)
    self.assertEqual(h.labelType, 'between')
    self.assertEqual(h.labels,
                     ['0.013', '0.020', '0.031', '0.048', '0.074', '0.114', '0.175', '0.269', '0.413', '0.635', '0.976'])
    self.assertEqual(h.sizes, [3, 2, 1, 2, 7, 9, 8, 19, 29, 43])

  def test_attributes(self):
    p = lynx.LynxKite().new_project()
    p.exampleGraph()
    md = p.meta_data()
    self.assertEqual(len(md.edgeAttributes), 2)
    self.assertEqual(len(md.vertexAttributes), 6)
    titles=[]
    for a in md.vertexAttributes:
      titles.append(a.title)
    self.assertEqual(titles, [
      'age',
      'gender',
      'id',
      'income',
      'location',
      'name'])
    titles=[]
    for a in md.edgeAttributes:
      titles.append(a.title)
    self.assertEqual(titles,['comment', 'weight'])


if __name__ == '__main__':
  unittest.main()
