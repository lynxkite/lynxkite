import unittest
import lynx
import json

ANCHOR_AND_EXAMPLE = '''
  [{"id":"anchor","operationId":"Anchor","parameters":{},"x":0,"y":0,"inputs":{},
  "parametricParameters":{}},{"id":"eg0","operationId":"Create example graph","parameters":{},
  "x":100,"y":100,"inputs":{},"parametricParameters":{}}]'''


class TestWorkspace(unittest.TestCase):

  def test_example_graph(self):
    lk = lynx.LynxKite()
    outputs = lk.run(json.loads(ANCHOR_AND_EXAMPLE))
    self.assertEqual(1, len(outputs))
    o = outputs[0]
    self.assertEqual(o.boxOutput.boxId, 'eg0')
    self.assertEqual(o.boxOutput.id, 'project')
    self.assertEqual(o.kind, 'project')
    self.assertTrue(o.success.enabled)

  def test_state_access(self):
    lk = lynx.LynxKite()
    outputs = lk.run(json.loads(ANCHOR_AND_EXAMPLE))
    state = outputs[0].stateId
    project = lk._ask('getProjectOutput', dict(id=state, path=''))

    def get_scalar(guid):
      return lk._ask('scalarValue', dict(scalarId=guid))

    scalars = {s.title: get_scalar(s.id) for s in project.scalars}
    self.assertEqual(scalars['!vertex_count'].double, 4.0)
    self.assertEqual(scalars['!edge_count'].double, 4.0)
    self.assertEqual(scalars['greeting'].string, 'Hello world! 😀 ')
