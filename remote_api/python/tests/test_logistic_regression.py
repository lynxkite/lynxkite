import unittest
import lynx
import os
import sqlite3


class TestLogisticRegression(unittest.TestCase):
  lk = lynx.LynxKite()
  # To see, how this test data was obtained,
  # check https://github.com/biggraph/biggraph/issues/5501
  expected = [
      {'prediction': 0.0, 'f1': 1.0, 'f2': 10.0, 'accept': 0.0},
      {'prediction': 1.0, 'f1': 3.0, 'f2': 7.0, 'accept': 0.0},
      {'prediction': 1.0, 'f1': 1.0, 'f2': 5.0, 'accept': 1.0},
      {'prediction': 1.0, 'f1': 2.0, 'f2': 1.0, 'accept': 1.0},
      {'prediction': 0.0, 'f1': 0.0, 'f2': 9.0, 'accept': 1.0},
      {'prediction': 1.0, 'f1': 8.0, 'f2': 8.0, 'accept': 1.0},
      {'prediction': 1.0, 'f1': 0.0, 'f2': 8.0, 'accept': 1.0},
      {'prediction': 0.0, 'f1': 2.0, 'f2': 10.0, 'accept': 0.0},
      {'prediction': 1.0, 'f1': 1.0, 'f2': 7.0, 'accept': 1.0},
      {'prediction': 1.0, 'f1': 3.0, 'f2': 1.0, 'accept': 1.0}]

  def test_training(self):
    p = self.lk.new_project()
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

  def setup_table(self):
    path = os.path.abspath('tests/test.db')
    url = 'jdbc:sqlite:{}'.format(path)

    conn = sqlite3.connect(path)
    c = conn.cursor()
    c.executescript('''
    DROP TABLE IF EXISTS regression;
    CREATE TABLE regression
    (accept TEXT, f1 TEXT, f2 TEXT);
    INSERT INTO regression VALUES
    ('1','0','9'),
    ('1','0','8'),
    ('1','1','7'),
    ('1','1','5'),
    ('1','2','1'),
    ('1','3','1'),
    ('0','1','10'),
    ('0','2','10'),
    ('0','3','7'),
    ('1','8','8');
    ''')
    conn.commit()
    conn.close()

    self.lk._request('/ajax/discardAllReallyIMeanIt')
    view = self.lk.import_jdbc(
        jdbcUrl=url,
        jdbcTable='regression')
    table = view.to_table().name
    return table

  def test_classification(self):
    table = self.setup_table()

    # Model training
    train_p = self.lk.new_project()
    train_p.importVertices(**{'id-attr': 'id', 'table': table})
    train_p.vertexAttributeToDouble(attr='accept,f1,f2')
    train_p.trainALogisticRegressionModel(**{
        'features': 'f1,f2',
        'label': 'accept',
        'max-iter': 20,
        'name': 'logreg_model'})

    # Classification based on model
    classify_p = self.lk.new_project()
    classify_p.importVertices(**{'id-attr': 'id', 'table': table})
    classify_p.vertexAttributeToDouble(attr='accept,f1,f2')
    classify_p.copyScalarFromOtherProject(
        destScalarName='model',
        sourceProject=train_p.global_name(),
        sourceScalarName='logreg_model')
    classify_p.classifyVerticesWithAModel(
        model='{"modelName":"model","features":["f1","f2"]}',
        name='prediction')

    result = self.lk.sql('SELECT accept, prediction, f1, f2 FROM `p`', p=classify_p).take(20)
    self.assertListEqual(result, self.expected)


if __name__ == '__main__':
  unittest.main()
