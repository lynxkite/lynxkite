import requests
import re
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta
import itertools as it

from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, StructField, StringType
import pyspark.sql.functions as F


def get_spark():
  return SparkSession.builder.appName('fx').getOrCreate()


def _date_to_prev_year_month(date):
  '''
  E.g. '2019-01-03' -> '2018-12'.
  '''
  curr_month_date = datetime.strptime(date, '%Y-%m-%d')
  prev_month_date = curr_month_date - relativedelta(months=1)
  return prev_month_date.strftime('%Y-%m')


def _get_rates_from_exchangeratesapi(start_date, end_date):
  '''
  Given a start and end date, returns the rates data from exchangeratesapi.io.
  Base currency is EUR.
  Inputted date format is '%Y-%m-%d'.
  '''
  response = requests.get(
      f'https://api.exchangeratesapi.io/history?start_at={start_date}&end_at={end_date}&base=EUR')
  rates = response.json()['rates']

  return rates


def _first_and_last_workdays_of_month(year_month):
  '''
  Returns a tuple of the first and last business days of the inputted month.
  Inputted date format is '%Y-%m'.
  '''
  first_day_of_month = f'{year_month}-01'

  first_workday = pd.date_range(first_day_of_month, periods=1, freq='BMS')[0].strftime('%Y-%m-%d')
  last_workday = pd.date_range(first_day_of_month, periods=1, freq='BM')[0].strftime('%Y-%m-%d')

  return first_workday, last_workday


def check_data_availability(date):
  '''
  If data is available for the last business day of the previous month,
  then we can assume data is available for every desired day of that month.
  So, this finds the last business day and checks if there's data for it.
  If available, returns True.
  If unavailable, returns False.
  '''
  year_month = _date_to_prev_year_month(date=date)

  _, last_business_day = _first_and_last_workdays_of_month(year_month=year_month)

  last_business_day_rates = _get_rates_from_exchangeratesapi(
      start_date=last_business_day, end_date=last_business_day)

  return bool(last_business_day_rates)


def rates_dict_to_df(rates_dict):
  '''
  Returns a spark dataframe from a dictionary such as
  the output of _get_rates_from_exchangeratesapi().
  '''
  spark = get_spark()

  row_list = [{**{'day': datetime.strptime(date, '%Y-%m-%d').day}, **rates}
              for date, rates in rates_dict.items()]

  df = spark.createDataFrame(Row(**dic) for dic in row_list)
  return df


def make_month_df(date):
  '''
  Returns a Spark dataframe of FX data for the full month of
  the given date.
  The dataframe is in the format:
  [day, currency1_to_eur_rate, eur_to_currency1_rate, ...,
  currencyn_to_eur_rate,eur_to_currencyn_rate]
  '''
  spark = get_spark()

  year_month = _date_to_prev_year_month(date=date)

  start_date, end_date = _first_and_last_workdays_of_month(year_month=year_month)

  full_month_rates = _get_rates_from_exchangeratesapi(start_date=start_date, end_date=end_date)

  df = rates_dict_to_df(rates_dict=full_month_rates)

  # Rename columns
  column_name_mapping = {
      'day': 'day', **{curr: f'eur_to_{curr.lower()}_rate' for curr in df.columns if curr != 'day'}}
  new_names = [column_name_mapping[colname] for colname in df.columns]
  df = df.toDF(*new_names)

  # Add columns for inverted currency pair rates
  for colname in (col for col in df.columns if col != 'day'):
    second_currency = re.findall(r'eur_to_([a-z]{3})_rate', colname)[0]
    inverted_colname = f'{second_currency}_to_eur_rate'
    df = df.withColumn(inverted_colname, F.round(1 / (F.col(colname)), 4))

  base_pair_cols = [colname for colname in df.columns if colname.startswith('eur_to')]
  inverted_pair_cols = [colname for colname in df.columns if colname.endswith('to_eur_rate')]

  # Rearrange so that inverted pairs precede their counterparts
  month_df = df.select(
      ['day'] + [item for sublist in zip(inverted_pair_cols, base_pair_cols) for item in sublist])

  # Save to a parquet file
  filename = f'monthly_data/{year_month}/month_df.parquet'
  month_df.write.mode('overwrite').parquet(filename)


def make_min_max_parquet(date):
  '''
  Creates and saves a parquet file containing the timestamp of the month
  and the lowest and highest 'to EUR' rate in that month for each currency.
  '''
  spark = get_spark()

  year_month = _date_to_prev_year_month(date=date)

  month_df = spark.read.parquet(f'monthly_data/{year_month}/month_df.parquet')

  colnames = [colname for colname in month_df.columns if colname.endswith('to_eur_rate')]

  # Compute min and max of each column in colnames, and join them into one dataframe
  min_df = month_df.agg({colname: 'min' for colname in colnames})
  max_df = month_df.agg({colname: 'max' for colname in colnames})
  min_max_df = min_df.crossJoin(max_df)

  # Rename columns from 'min(colname)' and 'max(colname)' to 'colname_min' and 'colname_max'
  column_name_mapping = {**{colname: f'{colname[4:-1]}_min' for colname in min_max_df.columns if colname.startswith('min')},
                         **{colname: f'{colname[4:-1]}_max' for colname in min_max_df.columns if colname.startswith('max')}}
  new_names = [column_name_mapping[colname] for colname in min_max_df.columns]
  min_max_df = min_max_df.toDF(*new_names)

  # Add a 'timestamp' column to min_max_df, indicating the year and month
  timestamp_df = spark.createDataFrame([[year_month]], ['timestamp'])
  min_max_df = min_max_df.crossJoin(timestamp_df)

  # Put timestamp column on the left
  min_max_df = min_max_df.select(
      ['timestamp'] + [colname for colname in min_max_df.columns if colname != 'timestamp'])

  # Save to a parquet file
  filename = f'monthly_data/{year_month}/min_max_prices.parquet'
  min_max_df.write.mode('overwrite').parquet(filename)


def _find_best_buy_sell_profit_tup(day_rate_tups):
  '''
  Given a list of tuples containing day number and exchange rate,
  returns a tuple containing the most profitable day to buy, sell,
  and the associated profit
  '''
  # Make a dictionary of buy and sell days to their associated profit
  profit_dict = {(buy[0], sell[0]): sell[1] - buy[1]
                 for buy, sell in it.combinations(day_rate_tups, 2)}

  # (buy, sell) pair that yields the maximum profit
  best_buy_sell_tup = max(profit_dict, key=profit_dict.get)
  max_profit = profit_dict[best_buy_sell_tup]

  return best_buy_sell_tup + (max_profit,)


def make_max_profit_parquet(date):
  '''
  Creates and saves a parquet file containing the best dates to buy and sell
  currencies in order to maximize profit for that month.
  '''
  spark = get_spark()

  year_month = _date_to_prev_year_month(date=date)

  month_df = spark.read.parquet(f'monthly_data/{year_month}/month_df.parquet')

  # Initialize a dataframe
  schema = StructType([StructField('currency', StringType(), False),
                       StructField('buy_date', StringType(), True),
                       StructField('sell_date', StringType(), True),
                       StructField('profit', StringType(), True)])

  max_profit_df = spark.createDataFrame([], schema)

  base_pair_cols = (colname for colname in month_df.columns if colname.startswith('eur_to'))
  for colname in base_pair_cols:

    # Make list of tuples of the day number and exchange rate
    day_rate_tups = month_df.rdd.map(lambda x: (x['day'], x[colname]))\
                                .sortByKey('day')\
                                .collect()

    currency = re.findall(r'eur_to_([a-z]{3})_rate', colname)[0]
    buy_day, sell_day, profit = _find_best_buy_sell_profit_tup(day_rate_tups)

    new_row = spark.createDataFrame([(currency, buy_day, sell_day, profit)])

    max_profit_df = max_profit_df.union(new_row)

  # Define a udf to prepend the year and month to the day
  day_to_date_udf = F.udf(
      lambda day: f'{year_month}-{day}' if day is not None else None,
      StringType())

  # For transactions yielding no profit, change the buy and sell dates to None
  # Otherwise, apply day_to_date_udf()
  max_profit_df_good = max_profit_df\
      .withColumn('buy_date', F.when(F.col('profit') <= 0.0, None)
                  .otherwise(day_to_date_udf(max_profit_df['buy_date'])))\
      .withColumn('sell_date', F.when(F.col('profit') <= 0.0, None)
                  .otherwise(day_to_date_udf(max_profit_df['sell_date'])))\
      .drop('profit')

  # Save to a parquet file
  filename = f'monthly_data/{year_month}/max_profit.parquet'
  max_profit_df.write.mode('overwrite').parquet(filename)
