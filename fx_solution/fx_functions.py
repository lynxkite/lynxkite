import requests
import sys

import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta
from calendar import monthrange

from pyspark import SparkContext
from pyspark.sql import SparkSession, SQLContext, Row

import pyspark.sql.functions as F

import itertools as it
import numpy as np


# Start a Spark session
sc = SparkContext('local', 'currencies')
spark = SparkSession(sc)
sqlContext = SQLContext(sc)


def _subtract_one_month(date):
    '''
    E.g. '2019-07-02' -> '2019-06-02'.
    '''
    curr_month_date = datetime.strptime(date, '%Y-%m-%d')
    prev_month_date = curr_month_date - relativedelta(months=1)
    return prev_month_date.strftime('%Y-%m-%d')

def _get_rates_from_exchangeratesapi(start_date, end_date):
    '''
    Given a start and end date, returns the rates data from exchangeratesapi.io.
    Base currency is EUR.
    Inputted date format is '%Y-%m-%d'.
    '''
    response = requests.get(f'https://api.exchangeratesapi.io/history?start_at={start_date}&end_at={end_date}&base=EUR')
    rates = response.json()['rates']

    return rates

def _first_and_last_workdays_of_month(date):
    '''
    Returns a tuple of the first and last business days of the inputted month.
    Inputted date format is '%Y-%m-%d'.
    '''
    first_day_of_month = f'{date[:-2]}01' # E.g. 2019-07-04 -> 2019-07-01

    first_workday = pd.date_range(first_day_of_month, periods=1, freq='BMS')[0].strftime('%Y-%m-%d')
    last_workday = pd.date_range(first_day_of_month, periods=1, freq='BM')[0].strftime('%Y-%m-%d')

    return first_workday, last_workday

def check_data_availability(**kwargs):
    '''
    If data is available for the last business day of the previous month,
    then we can assume data is available for every desired day of that month.
    So, this finds the last business day and checks if there's data for it.
    If available, returns True.
    If unavailable, returns False.
    '''
    date = kwargs['ds']
    date = _subtract_one_month(date)

    _, last_business_day = _first_and_last_workdays_of_month(date)

    last_business_day_rates = _get_rates_from_exchangeratesapi(start_date=last_business_day, end_date=last_business_day)

    return bool(last_business_day_rates)

def make_month_df(**kwargs):
    '''
    Returns a Spark dataframe of FX data for the full month of
    the given date.
    The dataframe is in the format:
    [day, currency1_to_eur_rate, eur_to_currency1_rate, ...,
    currencyn_to_eur_rate,eur_to_currencyn_rate]
    '''
    date = kwargs['ds'] # Get date stamp from context
    date = _subtract_one_month(date)
    year_month = date[:-3]

    # Find the first and last workdays of the month
    start_date, end_date = _first_and_last_workdays_of_month(date)

    # Get the dictionary of exchange rates
    full_month_rates = _get_rates_from_exchangeratesapi(start_date=start_date, end_date=end_date)

    # Create a Spark dataframe from the full_month_rates dictionary
    row_list = [{**{'day': datetime.strptime(date, '%Y-%m-%d').day}, **rates} for date, rates in full_month_rates.items()]
    row_json = sc.parallelize(row_list)
    df = sqlContext.read.json(row_json)

    # Rename columns
    column_name_mapping = {'day': 'day', **{curr: f'eur_to_{curr.lower()}_rate' for curr in df.columns if curr!='day'}}
    new_names = [column_name_mapping[colname] for colname in df.columns]
    df = df.toDF(*new_names)

    # Rearrange 'day' to first column and sort by 'day'
    df = df.select(['day']+[colname for colname in df.columns if colname!='day']).orderBy('day')

    # Add columns for inverted currency pair rates
    for colname in df.columns[1:]:
        words_in_colname = colname.split('_')
        inverted_colname = f'{words_in_colname[2]}_to_{words_in_colname[0]}_rate' # E.g. 'usd_to_eur_rate' -> 'eur_to_usd_rate
        df = df.withColumn(inverted_colname, F.round(1/(F.col(colname)), 4))

    base_pair_cols = [colname for colname in df.columns if colname[:3]=='eur']
    inverted_pair_cols = [colname for colname in df.columns if colname[7:10]=='eur']

    # Rearrange so that inverted pairs precede their counterparts
    month_df = df.select(['day']+[item for sublist in zip(inverted_pair_cols, base_pair_cols) for item in sublist])

    # Save to a parquet file
    filename = f'monthly_data/{year_month}/month_df.parquet'
    month_df.write.mode('overwrite').parquet(filename)

def make_min_max_parquet(**kwargs):
    '''
    Creates and saves a parquet file containing the timestamp of the month
    and the lowest and highest 'to EUR' rate in that month for each currency.
    '''
    date = kwargs['ds'] # Get date stamp from context
    date = _subtract_one_month(date)
    year_month = date[:-3]

    month_df = spark.read.parquet(f'monthly_data/{year_month}/month_df.parquet')

    colnames = [colname for colname in month_df.columns if colname[7:10]=='eur']

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
    timestamp_df = sqlContext.createDataFrame([[year_month]],['timestamp'])
    min_max_df = min_max_df.crossJoin(timestamp_df)

    # Put timestamp column on the left
    min_max_df = min_max_df.select(['timestamp']+[colname for colname in min_max_df.columns if colname!='timestamp'])

    # Save to a parquet file
    filename = f'monthly_data/{year_month}/min_max_prices.parquet'
    min_max_df.write.mode('overwrite').parquet(filename)

def _max_profit_from_col(year_month, month_df, colname):
    '''
    A helper function that finds the buy and sell dates yielding the maximum
    possible profit in a given currency and month.
    Used in make_max_profit_parquet().
    '''
    days_as_list = month_df.select('day').rdd.map(lambda row : row[0]).collect()
    col_as_list = month_df.select(colname).rdd.map(lambda row : row[0]).collect()

    # Find the days to buy and sell on which yield the largest profit
    day_price_tups = list(zip(days_as_list, col_as_list))
    profit_dict = {(buy[0], sell[0]): sell[1]-buy[1] for buy, sell in it.combinations(day_price_tups, 2)}

    best_buy_sell_tup = max(profit_dict, key=profit_dict.get)
    max_profit = profit_dict[best_buy_sell_tup]

    if max_profit <= 0: # If positive profit is impossible (i.e. rate only decreases)
        buy_day, sell_day = None, None
    else:
        buy_day, sell_day = best_buy_sell_tup

    buy_date = f'{year_month}-{str(buy_day)}'
    sell_date = f'{year_month}-{str(sell_day)}'

    return {'currency': colname[7:10], 'buy_date': buy_date, 'sell_date': sell_date}

def make_max_profit_parquet(**kwargs):
    '''
    Creates and saves a parquet file containing the best dates to buy and sell
    currencies in order to maximize profit for that month.
    Calls _max_profit_from_col().
    '''
    date = kwargs['ds'] # Get date stamp from context
    date = _subtract_one_month(date)
    year_month = date[:-3]

    month_df = spark.read.parquet(f'monthly_data/{year_month}/month_df.parquet')

    base_pair_cols = [colname for colname in month_df.columns if colname[:3]=='eur']

    df = month_df.select(['day'] + base_pair_cols)

    row_list = [Row(**_max_profit_from_col(year_month=year_month, month_df=df, colname=pair_col))
                for pair_col in base_pair_cols]

    max_profit_df = spark.createDataFrame(row_list)

    # Move the currency column to be on the left
    max_profit_df = max_profit_df.select(['currency']+[col for col in max_profit_df.columns if col!='currency'])

    # Save to a parquet file
    filename = f'monthly_data/{year_month}/max_profit.parquet'
    max_profit_df.write.mode('overwrite').parquet(filename)
