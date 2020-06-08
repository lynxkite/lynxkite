import unittest
import lynx.kite
import json
import time
from lynx.kite import subworkspace


class TestWorkspaceBuilder(unittest.TestCase):

  def test_one_box_ws(self):
    lk = lynx.kite.LynxKite()
    # Using explicit output name for test.
    graph = lk.createExampleGraph()['graph'].get_graph()
    scalars = {s.title: lk.get_graph_attribute(s.id) for s in graph.graphAttributes}
    self.assertEqual(scalars['!vertex_count'].double, 4.0)
    self.assertEqual(scalars['!edge_count'].double, 4.0)
    self.assertEqual(scalars['greeting'].string, 'Hello world! 😀 ')

  def test_numeric_box_parameter(self):
    lk = lynx.kite.LynxKite()
    s = lk.createVertices(size=6)
    res = lk.get_state_id(s)
    scalars = {s.title: lk.get_graph_attribute(s.id)
               for s in lk.createVertices(size=6).get_graph().graphAttributes}
    self.assertEqual(scalars['!vertex_count'].double, 6.0)

  def test_simple_chain(self):
    lk = lynx.kite.LynxKite()
    table = lk.createExampleGraph().computePageRank().sql(
        'select page_rank from vertices').get_table_data()
    self.assertEqual(table.header[0].dataType, 'Double')
    self.assertEqual(table.header[0].name, 'page_rank')
    values = [row[0].string for row in table.data]
    self.assertEqual(values, ['1.80917', '1.80917', '0.19083', '0.19083'])

  def test_simple_sql_chain(self):
    lk = lynx.kite.LynxKite()
    state = (lk.createExampleGraph()
             .sql('select * from vertices where age < 30')
             .sql('select name from input where age > 2'))
    table = state.get_table_data()
    values = [row[0].string for row in table.data]
    self.assertEqual(values, ['Adam', 'Eve'])

  def test_multi_input(self):
    lk = lynx.kite.LynxKite()
    eg = lk.createExampleGraph()
    new_edges = eg.sql('select * from edges where edge_weight > 1')
    new_graph = lk.useTableAsEdges(
        eg, new_edges, attr='id', src='src_id', dst='dst_id')
    graph = new_graph.get_graph()
    scalars = {s.title: lk.get_graph_attribute(s.id) for s in graph.graphAttributes}
    self.assertEqual(scalars['!vertex_count'].double, 4.0)
    self.assertEqual(scalars['!edge_count'].double, 3.0)

  def test_pedestrian_custom_box(self):
    lk = lynx.kite.LynxKite()
    i = lk.input(name='graph')
    o = i.sql('select name from vertices').output(name='vtable')
    ws = lynx.kite.Workspace(name='allvs', terminal_boxes=[o], input_boxes=[i])
    table = ws(lk.createExampleGraph()).get_table_data()
    values = [row[0].string for row in table.data]
    self.assertEqual(values, ['Adam', 'Eve', 'Bob', 'Isolated Joe'])

  def test_save_under_root(self):
    lk = lynx.kite.LynxKite()
    state = lk.createExampleGraph().sql('select name from vertices')
    ws = lynx.kite.Workspace([state], name='eg_names')
    lk.remove_name('save_it_under_this_folder/eg_names', force=True)
    lk.fetch_workspace_output_states(ws, 'save_it_under_this_folder')
    entries = lk.list_dir('save_it_under_this_folder')
    self.assertTrue('save_it_under_this_folder/eg_names' in {e.name for e in entries})

  def test_save_under_root_with_empty_string(self):
    lk = lynx.kite.LynxKite()
    eg = lk.createExampleGraph()
    lk.remove_name('just the eg', force=True)
    ws = lynx.kite.Workspace([eg], name='just the eg')
    # Saving to the "root" directory
    lk.save_workspace_recursively(ws, '')
    entries = lk.list_dir('')
    self.assertTrue('just the eg' in {e.name for e in entries})

  def test_parametric_parameters(self):
    from lynx.kite import pp
    lk = lynx.kite.LynxKite()
    graph = lk.createExampleGraph().deriveGraphAttribute(
        output='pi', expr=pp('${2+1.14}')).get_graph()
    scalars = {s.title: lk.get_graph_attribute(s.id) for s in graph.graphAttributes}
    self.assertEqual(scalars['pi'].string, '3.14')

  def parametric_ws(self):
    from lynx.kite import pp, text
    lk = lynx.kite.LynxKite()
    state = lk.createExampleGraph().sql(
        pp('select name from `vertices` where age = $ap')).output(name='table')
    ws = lynx.kite.Workspace([state], name='ws params', ws_parameters=[text('ap', '18.2')])
    return ws

  def test_parametric_parameters_with_defaults(self):
    lk = lynx.kite.LynxKite()
    ws = self.parametric_ws()
    table = ws().get_table_data()
    values = [row[0].string for row in table.data]
    self.assertEqual(values, ['Eve'])

  def test_parametric_parameters_with_workspace_parameters(self):
    lk = lynx.kite.LynxKite()
    ws = self.parametric_ws()
    table = ws(ap=20.3).get_table_data()
    values = [row[0].string for row in table.data]
    self.assertEqual(values, ['Adam'])

  def test_wrong_chain_with_multiple_inputs(self):
    lk = lynx.kite.LynxKite()
    with self.assertRaises(Exception) as context:
      state = lk.createExampleGraph().sql2(sql='select * from vertices')
    self.assertTrue('sql2 has more than one input' in str(context.exception))

  def test_trigger_box_with_save_snapshot(self):
    lk = lynx.kite.LynxKite()
    box = (lk.createExampleGraph()
           .sql('select name from vertices')
           .saveToSnapshot(path='this_is_my_snapshot'))
    lk.remove_name('trigger-folder', force=True)
    lk.remove_name('this_is_my_snapshot', force=True)
    ws = lynx.kite.Workspace([box], name='trigger-test')
    lk.save_workspace_recursively(ws, 'trigger-folder')
    # The boxId of the "Save to snapshot box" is saveToSnapshot_0
    lk.trigger_box('trigger-folder/trigger-test', 'saveToSnapshot_0')
    entries = lk.list_dir('')
    self.assertTrue('this_is_my_snapshot' in {e.name for e in entries})

  def test_trigger_box_with_manual_box_id(self):
    lk = lynx.kite.LynxKite()
    box = (lk.createExampleGraph()
           .sql('select name from vertices')
           .saveToSnapshot(path='this_is_my_snapshot2', _id='sts_to_trigger'))
    lk.remove_name('trigger-folder2', force=True)
    lk.remove_name('this_is_my_snapshot2', force=True)
    ws = lynx.kite.Workspace([box], name='trigger-test2')
    lk.save_workspace_recursively(ws, 'trigger-folder2')
    lk.trigger_box('trigger-folder2/trigger-test2', 'sts_to_trigger')
    entries = lk.list_dir('')
    self.assertTrue('this_is_my_snapshot2' in {e.name for e in entries})

  def test_conflicting_manual_box_ids(self):
    lk = lynx.kite.LynxKite()
    box1 = lk.createExampleGraph(_id='duplicate_id')
    box2 = lk.createExampleGraph(_id='duplicate_id')
    with self.assertRaises(Exception) as cm:
      ws = lynx.kite.Workspace([box1, box2], name='id-conflict-test')
    self.assertTrue('Duplicate box id(s): [\'duplicate_id\']' in str(cm.exception))
    sql_box = lk.createExampleGraph().sql('select * from vertices')
    conflicting_box = lk.createExampleGraph(_id='sql1_0')
    with self.assertRaises(Exception) as cm:
      ws = lynx.kite.Workspace([sql_box, conflicting_box], name='id-conflict-test2')
    self.assertTrue('Duplicate box id(s): [\'sql1_0\']' in str(cm.exception))

  def test_manual_box_ids_of_custom_boxes(self):
    lk = lynx.kite.LynxKite()

    @lk.workspace()
    def random_graph():
      return dict(g=lk.createVertices().createRandomEdges())

    box1 = random_graph(_id='rnd_g1')
    box2 = random_graph(_id='rnd_g2')
    ws = lynx.kite.Workspace([box1, box2], name='custom-box-manual-ids')
    self.assertEqual(
        {box['id'] for box in ws.to_json(
            workspace_root='manual_ids_folder',
            subworkspace_path='')}, {'anchor', 'rnd_g1', 'rnd_g2'})

    @subworkspace
    def page_rank(g):
      return g.computePageRank()

    box3 = page_rank(box1, _id='page_rank')
    ws2 = lynx.kite.Workspace([box3], name='subworkspace-manual-ids')
    self.assertEqual(
        {box['id'] for box in ws2.to_json(
            workspace_root='manual_ids_folder',
            subworkspace_path='')}, {'anchor', 'page_rank', 'rnd_g1'})

  def test_trigger_box_with_multiple_snapshot_boxes(self):
    lk = lynx.kite.LynxKite()
    eg = lk.createExampleGraph()
    b1 = eg.sql('select name from vertices').saveToSnapshot(path='names_snapshot')
    b2 = eg.sql('select age from vertices').saveToSnapshot(path='ages_snapshot')
    lk.remove_name('names_snapshot', force=True)
    lk.remove_name('ages_snapshot', force=True)
    lk.remove_name('trigger-folder', force=True)
    ws = lynx.kite.Workspace([b1, b2], name='multi-trigger-test',
                             side_effect_paths=[lynx.kite.BoxPath(b1), lynx.kite.BoxPath(b2)])
    lk.fetch_workspace_output_states(ws, 'trigger-folder')
    for box_id in [box['id']
                   for box in ws.to_json('trigger-folder/', ws.name)
                   if box['operationId'] == 'Save to snapshot']:
      lk.trigger_box('trigger-folder/multi-trigger-test', box_id)
    entries = lk.list_dir('')
    self.assertTrue('names_snapshot' in {e.name for e in entries})
    self.assertTrue('ages_snapshot' in {e.name for e in entries})

  def test_compute_state(self):
    def num_computed(state):
      return sum(1 for p in state.get_progress() if p == 1)

    def num_not_yet_started(state):
      return sum(1 for p in state.get_progress() if p == 0)

    lk = lynx.kite.LynxKite()

    # To be able to run the test manually and repeatedly,
    # we use a timestamp based seed.
    seed = str(int(round(time.time() * 1000)))[-8:]

    # Graph
    g = lk.createVertices().createRandomEdges(seed=seed).computePageRank()
    self.assertTrue(num_not_yet_started(g) > 0)
    g.compute()
    self.assertEqual(num_not_yet_started(g), 0)

    # Table
    g = lk.createVertices().createRandomEdges(seed=seed).computeDegree()
    state = g.sql('select degree from vertices')
    self.assertEqual(num_computed(state), 0)
    state.compute()
    self.assertEqual(num_computed(state), 1)

    # Visualization
    # Here we just test that we can call compute on a visualization
    visualization = '''{
    "left":{"projectPath":"","graphMode":"sampled","display":"svg",
    "filters":{"vertex":{},"edge":{}},
    "bucketCount":4,"preciseBucketSizes":false,"relativeEdgeDensity":false,
    "axisOptions":{"vertex":{},"edge":{}},
    "sampleRadius":1,"attributeTitles":{"label":"centrality"},
    "animate":{"enabled":false,"style":"expand","labelAttraction":0},
    "centers":["auto"],"customVisualizationFilters":false,"sliderPos":50},
    "right":{"display":"svg","filters":{"vertex":{},"edge":{}},
    "bucketCount":4,"preciseBucketSizes":false,"relativeEdgeDensity":false,
    "axisOptions":{"vertex":{},"edge":{}},
    "sampleRadius":1,"attributeTitles":{},
    "animate":{"enabled":false,"style":"expand","labelAttraction":0},
    "centers":["auto"],"customVisualizationFilters":false}}'''
    g = lk.createVertices().createRandomEdges(seed=seed).computeCentrality()
    v = g.graphVisualization(state=visualization)
    self.assertTrue(num_not_yet_started(g) > 0)
    v.compute()
    self.assertEqual(num_not_yet_started(g), 0)

    # Plot
    state = g.sql1().customPlot()
    self.assertEqual(num_computed(state), 0)
    state.compute()
    self.assertEqual(num_computed(state), 1)

  def test_builder_import(self):
    lk = lynx.kite.LynxKite()
    csv_path = lk.upload('a,b,c\n1,2,3\n4,5,6\n')
    table = lk.importCSVNow(filename=csv_path).sql('select * from input').get_table_data()
    self.assertEqual([[f.string for f in row]
                      for row in table.data], [['1', '2', '3'], ['4', '5', '6']])

  def test_builder_export_csv_with_generated_path(self):
    lk = lynx.kite.LynxKite()
    eg_table = (lk.createExampleGraph()
                .sql('select name, age, income from vertices')
                .exportToCSV())
    path = eg_table.run_export()
    data = lk.download_file(path)
    self.assertEqual(
        data, b'name,age,income\nAdam,20.3,1000.0\nEve,18.2,""\nBob,50.3,2000.0\nIsolated Joe,2.0,""\n')

  def test_builder_export_json_with_path_parameter(self):
    lk = lynx.kite.LynxKite()
    path = 'DATA$/export_tests/name_and_age_json'
    name_and_age = (lk.createExampleGraph()
                    .sql('select name, age from vertices where age < 30')
                    .exportToJSON(path=path))
    name_and_age.trigger()
    data = lk.download_file(path)
    self.assertEqual(
        data, b'{"name":"Adam","age":20.3}\n{"name":"Eve","age":18.2}\n{"name":"Isolated Joe","age":2.0}\n')

  def test_export_with_overwrite(self):
    lk = lynx.kite.LynxKite()
    path = 'DATA$/export_tests/to_overwrite'
    eg = lk.createExampleGraph().sql('select name, income from vertices')
    eg_export = eg.exportToJSON(path=path, save_mode="overwrite")
    eg_export.trigger()
    eg_export2 = eg.exportToJSON(path=path, version="2", save_mode="overwrite")
    eg_export2.trigger()
    data = lk.download_file(path)
    self.assertEqual(
        data,
        b'{"name":"Adam","income":1000.0}\n{"name":"Eve"}\n{"name":"Bob","income":2000.0}\n{"name":"Isolated Joe"}\n')

  def test_export_with_append(self):
    lk = lynx.kite.LynxKite()
    path = 'DATA$/export_tests/to_append'
    eg = lk.createExampleGraph().sql('select name from vertices limit 1')
    eg_export = eg.exportToJSON(path=path)
    eg_export.trigger()
    eg_export2 = eg.exportToJSON(path=path, version="2", save_mode="append")
    eg_export2.trigger()
    data = lk.download_file(path)
    self.assertEqual(data, b'{"name":"Adam"}\n{"name":"Adam"}\n')

  def test_builder_export_idempotent(self):
    lk = lynx.kite.LynxKite()
    eg = lk.createExampleGraph().sql('select * from vertices').exportToJSON()
    path = eg.run_export()
    path2 = eg.run_export()
    self.assertEqual(path, path2)

  def test_builder_export_wrong_prefix(self):
    lk = lynx.kite.LynxKite()
    path = 'WRONG_PREFIX$/table'
    names = (lk.createExampleGraph()
             .sql('select name from vertices')
             .exportToCSV(path=path))
    with self.assertRaises(lynx.kite.LynxException) as cm:
      names.trigger()
    self.assertTrue('Unknown prefix symbol: WRONG_PREFIX$' in str(cm.exception))

  def test_missing_function(self):
    lk = lynx.kite.LynxKite()
    with self.assertRaises(AttributeError) as cm:
      lk.createExampleGraph().notExists()
    self.assertEqual(
        str(cm.exception),
        "notExists is not defined on Operation createExampleGraph with parameters {} and inputs {}")

  def test_state_save_snapshot(self):
    lk = lynx.kite.LynxKite()
    eg = lk.createExampleGraph().sql('select name, income, gender from vertices')
    lk.remove_name('names_income_gender_snapshot', force=True)
    eg.save_snapshot('names_income_gender_snapshot')
    entries = lk.list_dir('')
    self.assertTrue('names_income_gender_snapshot' in {e.name for e in entries})

  def test_triggerable_boxes(self):
    lk = lynx.kite.LynxKite()
    eg = lk.createExampleGraph().sql('select name from vertices')
    snapshot = eg.saveToSnapshot(path='triggered save to snasphot')
    lk.remove_name('triggered save to snasphot', force=True)
    snapshot.trigger()
    entries = lk.list_dir('')
    self.assertTrue('triggered save to snasphot' in {e.name for e in entries})
    parquet = eg.exportToParquet(path='DATA$/triggered/parquet')
    parquet.trigger()
    data = lk.importParquetNow(filename='DATA$/triggered/parquet').get_table_data().data
    self.assertEqual([row[0].string for row in data], ['Adam', 'Eve', 'Bob', 'Isolated Joe'])

  def test_export_shorthand(self):
    lk = lynx.kite.LynxKite()
    eg = lk.createExampleGraph().sql('select name from vertices where income > 0')
    # state.exportNow
    eg.exportToParquetNow(path='DATA$/triggered/now/parquet1')
    data = lk.importParquetNow(filename='DATA$/triggered/now/parquet1').get_table_data().data
    self.assertEqual([row[0].string for row in data], ['Adam', 'Bob'])
    # lk.exportNow(state)
    lk.exportToParquetNow(eg, path='DATA$/triggered/now/parquet2')
    data = lk.importParquetNow(filename='DATA$/triggered/now/parquet2').get_table_data().data
    self.assertEqual([row[0].string for row in data], ['Adam', 'Bob'])
