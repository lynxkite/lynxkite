import unittest
import lynx.kite
import json


class TestSQLShorthand(unittest.TestCase):

  def test_sql_with_two_inputs(self):
    lk = lynx.kite.LynxKite()
    res = lk.sql('''select count(*) as cnt
                 from `one.vertices` cross join `two.vertices`''',
                 lk.createExampleGraph(),
                 lk.createExampleGraph())
    table = res.get_table_data()
    values = [row[0].double for row in table.data]
    self.assertEqual(values, [16.0])

  def test_sql_with_three_inputs(self):
    lk = lynx.kite.LynxKite()
    t1 = lk.createExampleGraph().sql('select name, age from vertices')
    t2 = lk.createExampleGraph().sql('select age, income from vertices where age <30')
    t3 = lk.createExampleGraph().sql('select income, name from vertices')

    res = lk.sql('''select one.name, two.age, three.income
                 from one, two, three
                 where one.age=two.age and two.income=three.income''',
                 t1, t2, t3)
    table = res.get_table_data()
    values = [[row[0].string, row[1].string] for row in table.data]
    self.assertEqual(values, [['Adam', '20.3']])

  def test_sql1_on_state(self):
    lk = lynx.kite.LynxKite()
    table = lk.createExampleGraph().sql('select name from vertices where age < 20').get_table_data()
    values = [row[0].string for row in table.data]
    self.assertEqual(values, ['Eve', 'Isolated Joe'])

  def test_table_to_pandas(self):
    import pandas as pd
    lk = lynx.kite.LynxKite()
    df = lk.createExampleGraph().sql('select name, age from vertices order by name').df()
    self.assertTrue(df.equals(pd.DataFrame([
        ['Adam', 20.3],
        ['Bob', 50.3],
        ['Eve', 18.2],
        ['Isolated Joe', 2],
    ], columns=['name', 'age'])))

  def test_limit(self):
    lk = lynx.kite.LynxKite()
    table = lk.createVertices(size=15).sql('select * from vertices').get_table_data()
    self.assertEqual(len(table.data), 15)
    small_table = lk.createExampleGraph().sql('select * from vertices').get_table_data(limit=2)
    self.assertEqual(len(small_table.data), 2)
    eg_table = lk.createExampleGraph().sql('select * from vertices').get_table_data(limit=8)
    self.assertEqual(len(eg_table.data), 4)
    import pandas as pd
    df = lk.createExampleGraph().sql('select name, age from vertices order by name').df(limit=3)
    self.assertTrue(df.equals(pd.DataFrame([
        ['Adam', 20.3],
        ['Bob', 50.3],
        ['Eve', 18.2],
    ], columns=['name', 'age'])))

  def test_kwarg_inputs(self):
    lk = lynx.kite.LynxKite()
    eg = lk.createExampleGraph()
    df = lk.sql(
        'select count(*) as cnt from `a.vertices` join `b.vertices` using (name)',
        a=eg, b=eg, persist='yes').df()
    import pandas as pd
    self.assertTrue(df.equals(pd.DataFrame({'cnt': [4.0]})))
