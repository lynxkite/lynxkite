import unittest
import os
import lynx.kite
from ruamel.yaml import YAML


yaml = YAML()

DATA_DIR_PATH = os.path.join(
    os.path.dirname(os.path.realpath(__file__)), 'data')


class TutorialTestCase(unittest.TestCase):

    @classmethod
    def init_outputs(cls, yaml_workspace, input_data_filenames):
        """Import YAML workspace and input data, and fetch states.

        Assumption 1: the input data files are in CSV format.

        Assumption 2: YAML workspace contains import-CSV-boxes with IDs
        'Import-CSV_1'...'Import-CSV_n' and the input data files should be
        imported in the same order.
        """
        with open(os.path.join(DATA_DIR_PATH, yaml_workspace)) as yaml_file:
            ws_yaml = yaml.load(yaml_file)

        cls.lk = lynx.kite.LynxKite()

        boxes = ws_yaml
        for i, data_filename in enumerate(input_data_filenames, 1):
            data_path = os.path.join(DATA_DIR_PATH, data_filename)
            with open(data_path, 'rb') as data_file:
                csv_box = next(x for x in boxes
                               if x['id'] == 'Import-CSV_{}'.format(i))
                csv_box['parameters'][
                    'filename'] = cls.lk.upload(data_file.read())
                boxes = cls.lk.import_box(boxes, 'Import-CSV_{}'.format(i))

        cls.outputs = cls.lk.fetch_states(boxes)

    @classmethod
    def get_project_scalars(cls, box_id, box_output_id, project_path=""):
        state = cls.outputs[box_id, box_output_id].stateId
        project = cls.lk.get_project(state, project_path)
        return {s.title: cls.lk.get_scalar(s.id) for s in project.scalars}


class TestTutorial2(TutorialTestCase):

    @classmethod
    def setUpClass(cls):
        cls.init_outputs(
            'tutorial-02-test.yaml',
            ('beno_facebook_vertices.csv', 'beno_facebook_edges.csv'))

    def test_graph_visualization_1(self):
        scalars = self.get_project_scalars(
            'Graph-visualization_1',
            'visualization')
        self.assertEqual(scalars['!vertex_count'].double, 403)
        self.assertEqual(scalars['!edge_count'].double, 3500)

    def test_graph_visualization_2(self):
        scalars = self.get_project_scalars(
            'Graph-visualization_2',
            'visualization')
        self.assertEqual(scalars['!vertex_count'].double, 402)
        self.assertEqual(scalars['!edge_count'].double, 3098)

    def test_find_infocom_communities_1(self):
        scalars = self.get_project_scalars(
            'Find-infocom-communities_1',
            'project',
            '.communities')
        self.assertEqual(scalars['!nonEmpty'].double, 37)
        self.assertEqual(scalars['!belongsToEdges'].double, 500)
        self.assertEqual(scalars['!coverage'].double, 349)

    def test_graph_visualization_6(self):
        scalars = self.get_project_scalars(
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
