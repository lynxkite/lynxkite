import unittest
import lynx.kite
import json


class TestWorkspaceBuilder(unittest.TestCase):

  def test_one_box_ws(self):
    lk = lynx.kite.LynxKite()
    state = lk.createExampleGraph()
    outputs = lk.run(state.to_json())
    state = outputs['Create-example-graph_0', 'project'].stateId
    project = lk.get_project(state)
    scalars = {s.title: lk.get_scalar(s.id) for s in project.scalars}
    self.assertEqual(scalars['!vertex_count'].double, 4.0)
    self.assertEqual(scalars['!edge_count'].double, 4.0)
    self.assertEqual(scalars['greeting'].string, 'Hello world! 😀 ')

  def test_simple_chain(self):
    lk = lynx.kite.LynxKite()
    state = lk.createExampleGraph().computePagerank().sql1(sql='select page_rank from `vertices`')
    outputs = lk.run(state.to_json())
    table_state = lk.run(state.to_json())['SQL1_0', 'table'].stateId
    table = lk.get_table(table_state)
    self.assertEqual(table.header[0].dataType, 'Double')
    self.assertEqual(table.header[0].name, 'page_rank')
    values = [row[0].string for row in table.data]
    self.assertEqual(values, ['1.80917', '1.80917', '0.19083', '0.19083'])

  def test_simple_sql_chain(self):
    lk = lynx.kite.LynxKite()
    state = (lk.createExampleGraph()
             .sql1(sql='select * from `vertices` where age < 30')
             .sql1(sql='select name from `input` where age > 2'))
    outputs = lk.run(state.to_json())
    table_state = lk.run(state.to_json())['SQL1_1', 'table'].stateId
    table = lk.get_table(table_state)
    values = [row[0].string for row in table.data]
    self.assertEqual(values, ['Adam', 'Eve'])

  def test_wrong_chain_with_multiple_inputs(self):
    lk = lynx.kite.LynxKite()
    with self.assertRaises(Exception) as context:
      state = lk.createExampleGraph().sql2(sql='select * from `vertices`')
    self.assertTrue('sql2 has more than one input' in str(context.exception))
