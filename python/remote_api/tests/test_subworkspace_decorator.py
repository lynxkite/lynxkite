import pandas as pd
import unittest
import lynx.kite
from lynx.kite import pp, ws_param, subworkspace, ws_name


class TestLazyWorkspaceDecorator(unittest.TestCase):

  def test_simplest(self):
    lk = lynx.kite.LynxKite()

    @subworkspace
    def select(x, column):
      return x.sql1(sql=f'select id, {column} from vertices order by id', persist='no')

    eg = lk.createExampleGraph()
    output = select(eg, 'name')
    expected = pd.DataFrame(
        {'id': ['0', '1', '2', '3'], 'name': ['Adam', 'Eve', 'Bob', 'Isolated Joe']})
    pd.testing.assert_frame_equal(output.df(), expected, check_like=True)

  def test_multiple_instances(self):
    lk = lynx.kite.LynxKite()

    @subworkspace
    def select(x, column):
      return x.sql1(sql=f'select id, {column} from vertices', persist='no')

    eg = lk.createExampleGraph()
    names = select(eg, 'name')
    ages = select(eg, 'age')
    result = lk.sql('select name, age from one join two where one.id = two.id order by name', ages, names, ages,
                    persist='no')
    expected = pd.DataFrame({
        'name': ['Adam', 'Bob', 'Eve', 'Isolated Joe'],
        'age': [20.3, 50.3, 18.2, 2]})
    pd.testing.assert_frame_equal(result.df(), expected, check_like=True)

  def test_recursive_instances(self):
    lk = lynx.kite.LynxKite()

    @ws_param('p')
    @subworkspace
    def f(x):
      return x.sql1(sql=pp('select $p from vertices'))

    @ws_param('p')
    @subworkspace
    def g(x):
      return f(x, p=pp('$p'))

    eg = lk.createExampleGraph()
    result = g(eg, p='name')
    expected = pd.DataFrame({'name': ['Adam', 'Eve', 'Bob', 'Isolated Joe']})
    pd.testing.assert_frame_equal(result.df(), expected, check_like=True)

  def test_input_naming(self):
    lk = lynx.kite.LynxKite()

    @subworkspace
    def f(i, *j, k, **l):
      return i

    eg = lk.createExampleGraph()
    result = f(eg, eg, eg, k=eg, l=eg, m=eg)
    self.assertEqual(list(result.inputs.keys()), ['i', 'j_1', 'j_2', 'k', 'l_l', 'l_m'])

  def test_varargs(self):
    lk = lynx.kite.LynxKite()

    @subworkspace
    def f1(i, *inputs):
      return inputs[i]

    eg = lk.createExampleGraph()
    empty = lk.createVertices(size=0)
    result = f1(2, empty, empty, eg, empty).sql('select name from vertices')
    expected = pd.DataFrame({'name': ['Adam', 'Eve', 'Bob', 'Isolated Joe']})
    pd.testing.assert_frame_equal(result.df(), expected, check_like=True)

    @subworkspace
    def f2(i, **inputs):
      return inputs[i]

    eg = lk.createExampleGraph()
    empty = lk.createVertices(size=0)
    result = f2('b', a=empty, b=eg).sql('select name from vertices')
    pd.testing.assert_frame_equal(result.df(), expected, check_like=True)

  def test_ws_params(self):
    lk = lynx.kite.LynxKite()

    @ws_param('name')
    @subworkspace
    def f(t):
      return t.sql1(sql=pp('select age from vertices where name == "$name"'))

    eg = lk.createExampleGraph()
    result = f(eg, name='Bob')
    expected = pd.DataFrame({'age': [50.3]})
    pd.testing.assert_frame_equal(result.df(), expected, check_like=True)

  def test_ws_multiple_params_with_defaults(self):
    lk = lynx.kite.LynxKite()

    @ws_param('name')
    @ws_param('column', default='age')
    @subworkspace
    def f(t):
      return t.sql1(sql=pp('select $column from vertices where name == "$name"'))

    eg = lk.createExampleGraph()
    result = f(eg, name='Bob')
    expected = pd.DataFrame({'age': [50.3]})
    pd.testing.assert_frame_equal(result.df(), expected, check_like=True)

  def test_ws_name(self):
    lk = lynx.kite.LynxKite()

    @ws_name('name')
    @subworkspace
    def f(t):
      return t

    eg = lk.createExampleGraph()
    result = f(eg)
    self.assertEqual(result.box_id_base(), 'name')

  def test_multi_output(self):
    lk = lynx.kite.LynxKite()

    @subworkspace
    def f(t):
      return dict(
          age=t.sql1(sql='select age from vertices limit 1'),
          name=t.sql1(sql='select name from vertices limit 1'))

    eg = lk.createExampleGraph()
    result = f(eg)
    pd.testing.assert_frame_equal(result['age'].df(), pd.DataFrame({'age': [20.3]}))
    pd.testing.assert_frame_equal(result['name'].df(), pd.DataFrame({'name': ['Adam']}))

  def test_sideeffects(self):
    lk = lynx.kite.LynxKite()

    @subworkspace
    def save(t, sec=lynx.kite.SideEffectCollector.AUTO):
      t.saveToSnapshot(path='sideeffect/saved').register(sec)

    lk.remove_name('sideeffect', force=True)
    save(lk.createExampleGraph()).trigger()
    self.assertEqual([e.name for e in lk.list_dir('sideeffect')], ['sideeffect/saved'])

    @lk.workspace_with_side_effects()
    def save_eg(sec):
      eg = lk.createExampleGraph()
      save(eg).register(sec)

    lk.remove_name('sideeffect', force=True)
    save_eg.trigger_all_side_effects()
    self.assertEqual([e.name for e in lk.list_dir('sideeffect')], ['sideeffect/saved'])

  def test_easy_ws_params(self):
    lk = lynx.kite.LynxKite()

    @ws_param('query')
    @subworkspace
    def f(t, query):
      return t.sql1(sql=query)

    @ws_param('column')
    @subworkspace
    def g(t):
      return f(t, query=pp('select $column from vertices limit 1'))

    eg = lk.createExampleGraph()
    result = g(eg, column='name')
    pd.testing.assert_frame_equal(result.df(), pd.DataFrame({'name': ['Adam']}))
