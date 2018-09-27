import unittest
import io
import pandas as pd
import lynx.kite


def dummy_data():
  return io.StringIO('''
food,best_sauce
hamburger,spicy mustard
french fries,samurai sauce
hot dog,ketchup with vinegar
fish and chips,tartar sauce''')


lk = lynx.kite.LynxKite()
dummy_table = lk.uploadCSVNow(dummy_data().read())
dummy_df = pd.read_csv(dummy_data())


class TestPandasConversion(unittest.TestCase):

  def assert_best_sauce_for_table(self, table, food, sauce):
    best_sauce = lk.sql(f'select best_sauce from input where food="{food}"',
                        table).get_table_data().data[0][0].string
    self.assertEqual(best_sauce, sauce)

  def assert_best_sauce_for_df(self, df, food, sauce):
    best_sauce = df.loc[df['food'] == food]['best_sauce'].iloc[0]
    self.assertEqual(best_sauce, sauce)

  def test_dummy_table(self):
    self.assert_best_sauce_for_table(dummy_table, 'hamburger', 'spicy mustard')

  def test_dummy_df(self):
    self.assert_best_sauce_for_df(dummy_df, 'french fries', 'samurai sauce')

  def test_pandas_dataframe_to_lk_table(self):
    table = lk.from_pandas_dataframe(dummy_df, include_index=False)
    table2 = lk.from_pandas_dataframe(dummy_df, include_index=True)
    self.assert_best_sauce_for_table(table, 'hot dog', 'ketchup with vinegar')
    self.assert_best_sauce_for_table(table2, 'hot dog', 'ketchup with vinegar')

  def test_lk_table_to_dataframe(self):
    df = dummy_table.df()
    self.assert_best_sauce_for_df(df, 'fish and chips', 'tartar sauce')

  def test_df_to_table_to_df(self):
    df = lk.from_pandas_dataframe(dummy_df, include_index=False).df()
    self.assertEqual(df.to_csv(), dummy_df.to_csv())
