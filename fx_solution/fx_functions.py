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
    E.g. '2019-07-02' -> '2019-06-02'
    '''
    curr_month_date = datetime.strptime(date, '%Y-%m-%d')
    prev_month_date = curr_month_date - relativedelta(months=1)
    return prev_month_date.strftime('%Y-%m-%d')


def _get_json(start_date, end_date):
    '''
    Given a start and end date, returns the json response from exchangeratesapi.io.
    Base currency is EUR. Date format is a string such as '2019-07-01'
    '''
    json_response = requests.get(
                'https://api.exchangeratesapi.io/history?start_at=' +
                start_date +
                '&end_at=' +
                end_date +
                '&base=' +
                'EUR'
                ).json()

    return json_response

def check_data_availability(**kwargs):
    '''
    If data is available for the last business day of the previous month,
    then we can assume data is available for every desired day of that month.
    So, this finds the last business day and checks if there's data for it.
    If available, returns True.
    If unavailable, returns False.
    '''
    date = kwargs.get('templates_dict').get('date') # Get date stamp from templates_dict

    date = _subtract_one_month(date) # Go back a month

    date_as_datetime = datetime.strptime(date, '%Y-%m-%d')

    # Find the first and last days of the previous month (possibly non-business days)
    start_date = date_as_datetime.replace(day=1)
    end_day = monthrange(date_as_datetime.year, date_as_datetime.month)[1] # Last day of the month
    end_date = date_as_datetime.replace(day=end_day)

    last_business_day = pd.date_range(start_date, end_date, freq='BM')[0].strftime('%Y-%m-%d')

    last_business_day_json = _get_json(start_date=last_business_day, end_date=last_business_day)

    return bool(last_business_day_json['rates']) # False if empty

def make_month_df(**kwargs):
    '''
    Returns a Spark dataframe of FX data for the full month of
    the given date.
    The dataframe is in the format:
    [day, currency1_to_eur_rate, eur_to_currency1_rate, ...,
    currencyn_to_eur_rate,eur_to_currencyn_rate]
    '''
    date = kwargs.get('templates_dict').get('date') # Get date stamp from templates_dict

    date = _subtract_one_month(date) # Go back a month
    date_as_datetime = datetime.strptime(date, '%Y-%m-%d')

    # Find the first and last days of the month
    start_date = date_as_datetime.replace(day=1).strftime('%Y-%m-%d')
    end_day = monthrange(date_as_datetime.year, date_as_datetime.month)[1]
    end_date = date_as_datetime.replace(day=end_day).strftime('%Y-%m-%d')

    full_month_json = _get_json(start_date=start_date, end_date=end_date) # API call

    full_month_rates = full_month_json['rates']

    # Create a Spark dataframe from the full_month_rates dictionary
    row_list = [{**{'day': datetime.strptime(date, '%Y-%m-%d').day}, **rates} for date, rates in full_month_rates.items()]
    row_json = sc.parallelize(row_list)
    df = sqlContext.read.json(row_json)

    # Rename columns
    column_name_mapping = {'day': 'day', **{curr: 'eur_to_' + curr.lower() + '_rate' for curr in df.columns if curr!='day'}}
    new_names = [column_name_mapping[col] for col in df.columns]
    df = df.toDF(*new_names)

    # Rearrange 'day' to first column and sort by 'day'
    df = df.select(['day']+[col for col in df.columns if col!='day']).orderBy('day')

    def _col_partner(colname):
        '''E.g. 'usd_to_eur_rate' -> 'eur_to_usd_rate'''
        word_list = colname.split('_')
        return word_list[2] + '_to_' + word_list[0] + '_rate'

    # Add columns for inverted currency pair rates
    for colname in df.columns[1:]:
        df = df.withColumn(_col_partner(colname=colname), F.round(1/(F.col(colname)), 4))

    base_pair_cols = [colname for colname in df.columns if colname[:3]=='eur']
    inverted_pair_cols = [colname for colname in df.columns if colname[7:10]=='eur']

    # Rearrange so that inverted pairs precede their counterparts
    month_df = df.select(['day']+[item for sublist in zip(inverted_pair_cols, base_pair_cols) for item in sublist])

    return month_df

def make_min_max_parquet(**kwargs):
    '''
    Creates and saves a parquet file containing the timestamp of the month
    and the lowest and highest 'to EUR' rate in that month for each currency.
    '''
    date = kwargs.get('templates_dict').get('date') # Get date stamp from templates_dict
    year_month = date[:-3] # We just want the month and year

    # month_df is passed in by make_month_df()
    month_df = kwargs['task_instance'].xcom_pull(task_ids='make_month_df_operator')


    colnames = [colname for colname in month_df.columns if colname[7:10]=='eur']

    # Find the minimum and maximum of each column
    summary_df = month_df.select(colnames).describe().where(F.col('summary').isin(['min','max']))
    min_row, max_row = summary_df.select(summary_df.columns[1:]).take(2)

    # Make the rows into dictionaries to turn them into columns in a Spark dataframe
    min_row_dict = {key+'_min': val for key, val in min_row.asDict().items()}
    max_row_dict = {key+'_max': val for key, val in max_row.asDict().items()}
    timestamp_dict = {'timestamp': year_month}

    row_list = [{**timestamp_dict, **min_row_dict, **max_row_dict}]
    json = sc.parallelize(row_list)
    min_max_df = sqlContext.read.json(json)

    # Move the timestamp column to be on the left
    min_max_df = min_max_df.select(['timestamp']+[col for col in min_max_df.columns if col!='timestamp'])

    # Save to a parquet file
    filename = 'DATA/min_max_prices_'+year_month+'.parquet'
    min_max_df.write.mode('overwrite').parquet(filename)
    print('Successfully saved file as ' + filename)
    return None

def _max_profit_from_col(year_month, month_df, colname):
    '''
    A helper function that finds the buy and sell dates yielding the maximum
    possible profit in a given currency and month.
    Used in make_max_profit_parquet().
    '''
    days_as_list = month_df.select('day').rdd.map(lambda row : row[0]).collect()
    col_as_list = month_df.select(colname).rdd.map(lambda row : row[0]).collect()

    # Find the days to buy and sell on which yield the largets profit
    day_price_tups = list(zip(days_as_list, col_as_list))
    profit_dict = {(buy[0], sell[0]): sell[1]-buy[1] for buy, sell in it.combinations(day_price_tups, 2)}

    best_buy_sell_tup = max(profit_dict, key=profit_dict.get)
    max_profit = profit_dict[best_buy_sell_tup]

    if max_profit <= 0: # If positive profit is impossible (i.e. rate only decreases)
        buy_day, sell_day = None, None
    else:
        buy_day, sell_day = best_buy_sell_tup

        buy_date = year_month + '-' + str(buy_day)
        sell_date = year_month + '-' + str(sell_day)

    return {'currency': colname[7:10], 'buy_date': buy_date, 'sell_date': sell_date}

def make_max_profit_parquet(month_df, **kwargs):
    '''
    Creates and saves a parquet file containing the best dates to buy and sell
    currencies in order to maximize profit for that month.
    Calls _max_profit_from_col().
    '''
    date = kwargs.get('templates_dict').get('date')
    year_month = date[:-3] # We just want the month and year

    # month_df is passed in by make_month_df(), fetch using xcom_pull
    month_df = kwargs['task_instance'].xcom_pull(task_ids='make_month_df_operator')

    base_pair_cols = [colname for colname in month_df.columns if colname[:3]=='eur']

    df = month_df.select(['day'] + base_pair_cols)

    row_list = [Row(**_max_profit_from_col(year_month=year_month, month_df=df, colname=pair_col))
                for pair_col in base_pair_cols]

    max_profit_df = spark.createDataFrame(row_list)

    # Move the currency column to be on the left
    max_profit_df = max_profit_df.select(['currency']+[col for col in max_profit_df.columns if col!='currency'])

    # Save to a parquet file
    filename = 'DATA/max_profit_'+year_month+'.parquet'
    max_profit_df.write.mode('overwrite').parquet(filename)
    print('Successfully saved file as ' + filename)
    return None
