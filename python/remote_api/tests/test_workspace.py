import unittest
import lynx.kite
import json

ANCHOR_AND_EXAMPLE = '''
  [{
    "id": "anchor",
    "operationId": "Anchor",
    "parameters": {},
    "x": 0, "y": 0,
    "inputs": {},
    "parametricParameters": {}
  }, {
    "id": "eg0",
    "operationId": "Create example graph",
    "parameters": {},
    "x": 100, "y": 100,
    "inputs": {},
    "parametricParameters": {}
  }]'''

IMPORT_SQL_EXPORT = '''
  [{
    "id": "anchor",
    "operationId": "Anchor",
    "parameters": {},
    "x": 0, "y": 0,
    "inputs": {},
    "parametricParameters": {}
  },
  {
    "id": "Import-CSV_1",
    "operationId": "Import CSV",
    "parameters": {
      "sql": "",
      "infer": "no",
      "imported_table": "<TABLE GUID>",
      "last_settings": "<LAST SETTINGS>",
      "imported_columns": "",
      "columns": "",
      "filename": "<FILENAME>",
      "error_handling": "FAILFAST",
      "limit": "",
      "delimiter": ","
    },
    "x": 381, "y": 419,
    "inputs": {},
    "parametricParameters": {}
  },
  {
    "id": "SQL1_1",
    "operationId": "SQL1",
    "parameters": {
      "sql": "<SQL QUERY>"
    },
    "x": 590, "y": 357,
    "inputs": { "input": { "boxId": "Import-CSV_1", "id": "table" } },
    "parametricParameters": {}
  },
  {
    "id": "Export-to-CSV_1",
    "operationId": "Export to CSV",
    "parameters": {
      "path": "<auto>",
      "quote": "",
      "version": "0",
      "header": "yes",
      "delimiter": ","
    },
    "x": 794, "y": 401,
    "inputs": { "table": { "boxId": "SQL1_1", "id": "table" } },
    "parametricParameters": {}
  }]'''


class TestWorkspace(unittest.TestCase):

  def test_example_graph(self):
    lk = lynx.kite.LynxKite()
    outputs = lk.fetch_states(json.loads(ANCHOR_AND_EXAMPLE))
    self.assertEqual(1, len(outputs))
    o = outputs['eg0', 'graph']
    self.assertEqual(o.boxOutput.boxId, 'eg0')
    self.assertEqual(o.boxOutput.id, 'graph')
    self.assertEqual(o.kind, 'graph')
    self.assertTrue(o.success.enabled)

  def test_state_access(self):
    lk = lynx.kite.LynxKite()
    outputs = lk.fetch_states(json.loads(ANCHOR_AND_EXAMPLE))
    state = outputs['eg0', 'graph'].stateId
    graph = lk.get_graph(state)
    scalars = {s.title: lk.get_scalar(s.id) for s in graph.scalars}
    self.assertEqual(scalars['!vertex_count'].double, 4.0)
    self.assertEqual(scalars['!edge_count'].double, 4.0)
    self.assertEqual(scalars['greeting'].string, 'Hello world! 😀 ')

  def test_import_sql_export(self):
    lk = lynx.kite.LynxKite()
    csv_path = lk.upload('a,b,c\n1,2,3\n4,5,6\n')
    workspace_json = IMPORT_SQL_EXPORT
    workspace_json = workspace_json.replace('<SQL QUERY>', 'select a, b + c as sum from input')
    workspace_json = workspace_json.replace('<FILENAME>', csv_path)
    boxes = json.loads(workspace_json)
    boxes = lk.import_box(boxes, 'Import-CSV_1')
    outputs = lk.fetch_states(boxes)
    # Check table.
    output = outputs['SQL1_1', 'table']
    table = lk.get_table_data(output.stateId)
    self.assertEqual([c.name for c in table.header], ['a', 'sum'])
    self.assertEqual([[f.string for f in row] for row in table.data], [['1', '5'], ['4', '11']])
    # Check export.
    export = lk.export_box(outputs, 'Export-to-CSV_1')
    data = lk.download_file(export.parameters.path)
    self.assertEqual(data, b'a,sum\n1,5.0\n4,11.0\n')

  def test_save_workspace(self):
    lk = lynx.kite.LynxKite()
    lk.remove_name('set_workspace_test', force=True)
    lk.save_workspace('set_workspace_test', json.loads(ANCHOR_AND_EXAMPLE))

  def test_save_snapshot(self):
    lk = lynx.kite.LynxKite()
    outputs = lk.fetch_states(json.loads(ANCHOR_AND_EXAMPLE))
    state = outputs['eg0', 'graph'].stateId
    lk.remove_name('save_snapshot_test', force=True)
    lk.save_snapshot('save_snapshot_test', state)
    entries = lk.list_dir('')
    self.assertTrue('save_snapshot_test' in [entry.name for entry in entries])

  def test_create_dir(self):
    lk = lynx.kite.LynxKite()
    lk.remove_name('my_dir', force=True)
    lk.create_dir('my_dir')
    lk.remove_name('my_dir/sub_dir', force=True)
    lk.create_dir('my_dir/sub_dir')
    entries = lk.list_dir('my_dir')
    self.assertEqual('my_dir/sub_dir', entries[0].name)
