import unittest
import os
import lynx.kite
from ruamel.yaml import YAML


yaml = YAML()


def get_project_scalars(lk, outputs, box_id, box_output_id, project_path=""):
    state = outputs[box_id, box_output_id].stateId
    project = lk.get_project(state, project_path)
    return {s.title: lk.get_scalar(s.id) for s in project.scalars}


class TestTutorial2(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        data_path = os.path.join(
            os.path.dirname(os.path.realpath(__file__)),
            'data')
        with open(os.path.join(data_path, 'tutorial-02-test.yaml')) as yaml_file:
            ws_yaml = yaml.load(yaml_file)

        cls.lk = lynx.kite.LynxKite()
        with open(os.path.join(data_path, 'beno_facebook_vertices.csv'), 'rb') as vertices_file, \
                open(os.path.join(data_path, 'beno_facebook_edges.csv'), 'rb') as edges_file:
            vertices_box = next(
                x for x in ws_yaml if x['id'] == 'Import-CSV_1')
            vertices_box['parameters'][
                'filename'] = cls.lk.upload(vertices_file)
            edges_box = next(x for x in ws_yaml if x['id'] == 'Import-CSV_2')
            edges_box['parameters']['filename'] = cls.lk.upload(edges_file)

        boxes = cls.lk.import_box(ws_yaml, 'Import-CSV_1')
        boxes = cls.lk.import_box(boxes, 'Import-CSV_2')
        cls.outputs = cls.lk.fetch_states(boxes)

    def test_graph_visualization_1(self):
        scalars = get_project_scalars(
            self.lk,
            self.outputs,
            'Graph-visualization_1',
            'visualization')
        self.assertEqual(scalars['!vertex_count'].double, 403)
        self.assertEqual(scalars['!edge_count'].double, 3500)

    def test_graph_visualization_2(self):
        scalars = get_project_scalars(
            self.lk,
            self.outputs,
            'Graph-visualization_2',
            'visualization')
        self.assertEqual(scalars['!vertex_count'].double, 402)
        self.assertEqual(scalars['!edge_count'].double, 3098)

    def test_find_infocom_communities_1(self):
        scalars = get_project_scalars(
            self.lk,
            self.outputs,
            'Find-infocom-communities_1',
            'project',
            '.communities')
        self.assertEqual(scalars['!nonEmpty'].double, 37)
        self.assertEqual(scalars['!belongsToEdges'].double, 500)
        self.assertEqual(scalars['!coverage'].double, 349)

    def test_graph_visualization_6(self):
        scalars = get_project_scalars(
            self.lk,
            self.outputs,
            'Graph-visualization_6',
            'visualization')
        self.assertEqual(scalars['!vertex_count'].double, 403)
        self.assertEqual(scalars['!edge_count'].double, 3500)

    def test_sql_1(self):
        output = self.outputs['SQL1_1', 'table']
        table = self.lk.get_table_data(output.stateId)
        self.assertEqual(
            [c.name for c in table.header],
            ['firstname',
             'page_rank'])
        self.assertEqual([[f.string for f in row] for row in table.data][0:10],
                         [['Beno', '24.20761'],
                          ['Alexandra', '4.63142'],
                          ['Szabina', '4.28497'],
                          ['Gyuri', '3.17637'],
                          ['Kinga', '2.81097'],
                          ['Gabor', '2.69417'],
                          ['Zsolt', '2.68994'],
                          ['Reka', '2.51839'],
                          ['Vagyim', '2.29865'],
                          ['Janos', '2.27615']
                          ])
