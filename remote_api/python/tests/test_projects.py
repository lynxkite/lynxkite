import unittest
import lynx


class TestProjects(unittest.TestCase):

  def test_save_and_load_project(self):
    lk = lynx.LynxKite()
    lk._request('/ajax/discardAllReallyIMeanIt')
    p = lk.new_project()
    p.exampleGraph()
    p.save('test_project')
    p2 = lk.load_project('test_project')
    self.assertTrue(p2.checkpoint is not None)
    self.assertEqual(p.checkpoint, p2.checkpoint)

  def test_get_scalar(self):
    lk = lynx.LynxKite()
    p = lk.new_project()
    p.exampleGraph()
    greeting = p.scalar('greeting')
    self.assertEqual(greeting, 'Hello world! 😀 ')

  def test_compute(self):
    p = lynx.LynxKite().new_project()
    p.newVertexSet(size=10)
    p.compute()
    self.assertTrue(p.is_computed())

  def test_table_name(self):
    p1 = lynx.LynxKite().new_project()
    p1.exampleGraph()
    p2 = lynx.LynxKite().new_project()
    p2.importVertices(**{'id-attr': 'id', 'table': p1.edges_table()})
    self.assertEqual(4, p2.scalar('vertex_count'))

  def test_project_operations(self):
    # Tests if the given operation names are valid LynxKite operation names.
    # Intentionally calls all the operations with a non-existent argument.
    lk = lynx.LynxKite()
    p = lk.new_project()
    # Getting operation names.
    dir(p)
    ops = lk._operation_names
    for op in ops:
      f = getattr(p, op)
      with self.assertRaises(Exception) as context:
        f(wrong_argument_name=444)
        self.assertFalse('No such operation' in str(context.exception))
        self.assertTrue('Extra parameters found' in str(context.exception))


if __name__ == '__main__':
  unittest.main()
