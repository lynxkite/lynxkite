import unittest
import lynx
import random


class TestLogisticRegression(unittest.TestCase):

  def test_training(self):
    # Random seed to force new computation in "standalone" test mode.
    # If we would run the test with fix seed, an earlier computed scalar
    # could cause the test to pass.
    test_seed = str(int(random.random() * 1000000))
    p = lynx.LynxKite().new_project()
    p.newvertexSet(size=100)
    p.addRandomVertexAttribute(**{
        'name': 'rnd',
        'dist': 'Standard Normal',
        'seed': test_seed})
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
    p.compute()


if __name__ == '__main__':
  unittest.main()
