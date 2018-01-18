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
    table_state = lk.get_state_id(res)
    table = lk.get_table(table_state)
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
    table_state = lk.get_state_id(res)
    table = lk.get_table(table_state)
    values = [[row[0].string, row[1].string] for row in table.data]
    self.assertEqual(values, [['Adam', '20.3']])

  def test_sql1_on_state(self):
    lk = lynx.kite.LynxKite()
    t = lk.createExampleGraph().sql('select name from vertices where age < 20')
    table_state = lk.get_state_id(t)
    table = lk.get_table(table_state)
    values = [row[0].string for row in table.data]
    self.assertEqual(values, ['Eve', 'Isolated Joe'])

  def test_table_to_pandas(self):
    import pandas as pd
    lk = lynx.kite.LynxKite()
    df = lk.createExampleGraph().sql('select name, age from vertices order by name').df(lk)
    self.assertTrue(df.equals(pd.DataFrame([
        ['Adam', 20.3],
        ['Bob', 50.3],
        ['Eve', 18.2],
        ['Isolated Joe', 2],
    ], columns=['name', 'age'])))
