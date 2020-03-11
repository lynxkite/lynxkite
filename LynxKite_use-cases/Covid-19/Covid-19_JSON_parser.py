# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.4.0
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# # Covid-19 edge and vertex creator
# _2020.03.11_<br/>
# This notebook creates edges and vertices CSV from the CSV downloaded from [sgwuhan.xose.net](https://sgwuhan.xose.net/api).
#
# 1. Download the output of [SGWuhan website JSON source](https://sgwuhan.xose.net/api) to data/sgwuhan.json
# 2. Run this notebook
# 3. Import data/vertices.csv and data/edges.csv source files into [Covid-19 virus spreading](https://pizzabox.lynxanalytics.com/#/workspace/Users/zoltan.balogh@lynxanalytics.com/Covid-19/virus_spreading) workspace

import sys
!{sys.executable} -m pip install jupytext

# +
import json

import numpy as np

import pandas as pd
from pandas.io.json import json_normalize
from pandas.tseries.offsets import DateOffset

from IPython.core.display import display, HTML
display(HTML("<style>.container { width:100% !important; }</style>"))

# +
with open('data/sgwuhan.json') as json_file:
    data = json.load(json_file)

df_input = json_normalize(data['data'])
# -

df_input['confirmDate_parsed'] = pd.to_datetime(df_input['confirmDate'], format='%d %b', errors='coerce')
df_input['confirmDate_parsed'] += DateOffset(years=120)

# Search for missing confirmed dates, then updating them to the maximum date.

df_input[(df_input['confirmDate_parsed'].isna()) & (df_input['caseType'].isin(['oldCase', 'newCase']))].index.values

max_confirm_date = df_input['confirmDate_parsed'].max()

df_input.loc[191, 'confirmDate_parsed'] = max_confirm_date
df_input.loc[192, 'confirmDate_parsed'] = max_confirm_date
df_input.loc[203, 'confirmDate_parsed'] = max_confirm_date
df_input.loc[272, 'confirmDate_parsed'] = max_confirm_date

df_input.drop(columns=['mohURL'], inplace=True)

df_input.replace({"caseType": {'oldCase': 'person', 'oldCaseCase': 'person', 'etcCase': 'location', 'updCase': 'person', 'newCase': 'person'}}, inplace=True) 

df_input.rename(columns={'caseType': 'vertex_type', 'confirmDate_parsed': 'confirm_date', 'lng': 'lon', 'caseNo': 'case_number'}, inplace=True)

df_input['confirm_day_of_year'] = df_input['confirm_date'].dt.dayofyear

# ## Vertices

df_input.reset_index(drop=False, inplace=True)
df_input.rename(columns={'index': 'id'}, inplace=True)

df_input[['id', 'case_number', 'vertex_type', 'gender', 'age', 'from', 'citizenship', 'confirm_date', 'confirm_day_of_year', 'lat', 'lon', 'location', 'stayed', 'visited']].to_csv('data/vertices.csv', index=False, sep=';')

# ## Edges

df_link = pd.DataFrame(columns=['from', 'to'])

for i in range(len(df_input)):
    if pd.isnull(df_input.iloc[i]['relatedArrayNo']) == False:
        links = df_input.iloc[i]['relatedArrayNo'].split(',')

        for k in range(len(links)):
            if links[k].isnumeric() & (df_input.iloc[i]['id'] != ''):
                df_link = df_link.append({
                    'from': int(i),
                    'to': int(links[k])
                }, ignore_index=True)

# Dropping wrong person-to-person and location-to-location links.

df_link.drop([18, 19, 20, 23, 24, 25, 28, 29, 30, 33, 34, 36, 37, 138, 139, 425], inplace=True)

df_link.to_csv('data/edges.csv', index=False, sep=';')
