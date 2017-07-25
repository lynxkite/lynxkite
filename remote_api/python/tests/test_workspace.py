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
    scalars = {s.title: lk.get_scalar(s.id) for s in project.scalars}
    self.assertEqual(scalars['!vertex_count'].double, 4.0)
    self.assertEqual(scalars['!edge_count'].double, 4.0)
    self.assertEqual(scalars['greeting'].string, 'Hello world! 😀 ')

  def test_import_sql_export(self):
    lk = lynx.LynxKite()
    csv_path = lk.upload('a,b,c\n1,2,3\n4,5,6\n')
    workspace_json = '''
    [{"id":"anchor","operationId":"Anchor","parameters":{},"x":0,"y":0,"inputs":{},
    "parametricParameters":{}},
    {"id":"Import-CSV_1","operationId":"Import CSV",
    "parameters":{"sql":"","infer":"no","imported_table":"<TABLE GUID>",
    "last_settings":"<LAST SETTINGS>", "imported_columns":"","columns":"","filename":"<FILENAME>",
    "error_handling":"FAILFAST","limit":"","delimiter":","},"x":381,"y":419,"inputs":{},
    "parametricParameters":{}},
    {"id":"SQL1_1","operationId":"SQL1","parameters":{
    "sql":"<SQL QUERY>"},"x":590,"y":357,"inputs":{"input":{"boxId":
    "Import-CSV_1","id":"table"}},"parametricParameters":{}},
    {"id":"Export-to-CSV_1","operationId":"Export to CSV","parameters":{"path":"<auto>","quote":"",
    "version":"0","header":"yes","delimiter":","},"x":794,"y":401,
    "inputs":{"table":{"boxId":"SQL1_1","id":"table"}},"parametricParameters":{}}]'''
    workspace_json = workspace_json.replace('<SQL QUERY>', 'select a, b + c as sum from input')
    workspace_json = workspace_json.replace('<FILENAME>', csv_path)
    import_result = lk._send('importBox', json.loads(workspace_json)[1])
    workspace_json = workspace_json.replace('<TABLE GUID>', import_result.guid)
    workspace_json = workspace_json.replace(
        '"<LAST SETTINGS>"', json.dumps(import_result.parameterSettings))
    outputs = lk.run(json.loads(workspace_json))
    outputs = {o.boxOutput.boxId: o for o in outputs}
    output = outputs['Export-to-CSV_1']
    self.assertEqual(output.kind, 'exportResult')
    self.assertTrue(output.success.enabled)
    export = lk._ask('getExportResultOutput', dict(stateId=output.stateId))
    scalar = lk.get_scalar(export.result.id)
    self.assertEqual(scalar.string, 'Export done.')
    data = lk._get(
        'downloadFile',
        params=dict(q=json.dumps(dict(path=export.parameters.path, stripHeaders=False)))).content
    self.assertEqual(data, b'a,sum\n1,5.0\n4,11.0\n')
