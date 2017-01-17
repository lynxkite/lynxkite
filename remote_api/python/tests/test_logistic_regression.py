import unittest
import lynx


class TestLogisticRegression(unittest.TestCase):

  def test_training(self):
    p = lynx.LynxKite().new_project()
    p.newvertexSet(size=100)
    p.addRandomVertexAttribute(**{
        'name': 'rnd',
        'dist': 'Standard Normal',
        'seed': '1234543'})
    p.derivedVertexAttribute(
        expr='rnd  > 0 ? 1 : 0',
        output='label',
        type='double')
    p.addRandomVertexAttribute(**{
        'name': 'feature',
        'dist': 'Standard Uniform',
        'seed': '1235321'})
    p.trainALogisticRegressionModel(**{
        'features': 'feature',
        'label': 'label',
        'max-iter': 20,
        'name': 'test'})
    # No assert, we only want to see if it runs without error.
    # This test try to catch an error caused by wrong breeze version.
    self.assertFalse(p.is_computed())
    p.compute()


if __name__ == '__main__':
  unittest.main()
