import unittest
import lynx.kite
import json


class TestSQLShorthand(unittest.TestCase):

  def test_sql_with_two_inputs(self):
    lk = lynx.kite.LynxKite()
    res = lk.sql(
        lk.createExampleGraph(),
        lk.createExampleGraph(),
        sql='select count(*) as cnt from `one.vertices` cross join `two.vertices`')
    table_state = lk.get_state_id(res)
    table = lk.get_table(table_state)
    values = [row[0].double for row in table.data]
    self.assertEqual(values, [16.0])

  def test_sql_with_three_inputs(self):
    lk = lynx.kite.LynxKite()
    t1 = lk.createExampleGraph().sql1(sql='select name, age from vertices')
    t2 = lk.createExampleGraph().sql1(sql='select age, income from vertices where age <30')
    t3 = lk.createExampleGraph().sql1(sql='select income, name from vertices')

    res = lk.sql(t1, t2, t3,
                 sql='select one.name, two.age, three.income from one, two, three where one.age=two.age and two.income=three.income')
    table_state = lk.get_state_id(res)
    table = lk.get_table(table_state)
    values = [[row[0].string, row[1].string] for row in table.data]
    self.assertEqual(values, [['Adam', '20.3']])
