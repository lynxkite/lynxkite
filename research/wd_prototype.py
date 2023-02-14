import backoff
import functools
import httpx
import os
import openai
import pandas as pd
from pprint import pprint
import shelve
import SPARQLWrapper
import sys

import prompts

pd.options.display.max_columns = None
shelf = shelve.open("wd.cache")
openai.api_key = os.getenv("OPENAI_API_KEY")
sparql = SPARQLWrapper.SPARQLWrapper('https://query.wikidata.org/sparql')
sparql.setReturnFormat(SPARQLWrapper.JSON)


def if_fails(msg):
  def if_fails(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
      try:
        return func(*args, **kwargs)
      except BaseException:
        pprint(args, kwargs)
        raise Exception(msg)
    return wrapper
  return if_fails


@backoff.on_exception(backoff.fibo, openai.error.RateLimitError)
def openai_complete(**kwargs):
  print('.')
  key = repr(kwargs)
  if key not in shelf:
    response = openai.Completion.create(**kwargs)
    shelf[key] = response.choices[0].text
  return shelf[key]


def openai_text(prompt, **kwargs):
  return openai_complete(
      model="text-davinci-003",
      prompt=prompt,
      temperature=0,
      max_tokens=500,
      **kwargs,
  )


def openai_code(prompt, **kwargs):
  return openai_complete(
      model="code-davinci-002",
      prompt=prompt,
      temperature=0,
      max_tokens=500,
      **kwargs,
  )


def search(type, q):
  key = type + q
  if key not in shelf:
    res = httpx.get(
        "https://www.wikidata.org/w/api.php",
        params={
            "action": "wbsearchentities",
            "language": "en",
            "format": "json",
            "search": q,
            "type": type,
        },
    )
    res = res.json()["search"]
    if not res:
      raise ValueError(f'WikiData cannot find {q}')
    shelf[key] = res[0]["id"]
  return shelf[key]


@if_fails('Failed to list the entities and properties corresponding to the query.')
def extract_entities(nodes, edge_condition):
  text = openai_text(
      prompts.ENTITY.replace("NODES", nodes).replace(
          "EDGE_CONDITION", edge_condition
      ),
      stop="----",
  )
  return [[x.strip() for x in xs.split("\n-")] for xs in text.split("Properties:\n-")]


@if_fails('WikiData failed to execute the SPARQL query. It is wrong or too complex.')
def run_sparkql(query):
  print(query)
  if query not in shelf:
    sparql.setQuery(query)
    shelf[query] = sparql.queryAndConvert()
  data = []
  for row in shelf[query]["results"]["bindings"]:
    data.append({k: v['value'] for k, v in row.items()})
  return pd.DataFrame(data)


@if_fails('Failed to create SPARQL query.')
def get_sparql(nodes, edge_condition, ids):
  ids_formatted = "\n".join(f"# - {e}: {id}" for (e, id) in ids)
  print(ids_formatted)
  return openai_code(
      prompts.SPARQL.replace("NODES", nodes)
      .replace("EDGE_CONDITION", edge_condition)
      .replace("IDS", ids_formatted),
      stop="####",
  ).strip()


def get_graph(nodes, edge_condition, limit):
  entities, properties = extract_entities(nodes, edge_condition)
  print(entities)
  print(properties)
  ids = [(e, 'wd:' + search("item", e)) for e in entities] + [
      (e, 'wdt:' + search("property", e)) for e in properties
  ]
  sparql = get_sparql(nodes, edge_condition, ids)
  df = run_sparkql(f'SELECT {sparql}' + (f'\nLIMIT {limit}' if limit else ''))
  print(df)


@if_fails('Failed to understand what nodes and edges correspond to the query.')
def split_query(query):
  text = openai_text(prompts.SPLIT_QUERY.replace('QUERY', query))
  [nodes, edge_condition] = [t.strip() for t in text.split('Connected: if')]
  print([nodes, edge_condition])
  return nodes, edge_condition


def easy_get_graph(query, limit):
  nodes, edge_condition = split_query(query)
  return get_graph(nodes, edge_condition, limit)


# get_graph("defense industry companies", "one is a subsidiary of the other")
# easy_get_graph("Disney characters by movies", 10)
easy_get_graph(' '.join(sys.argv[1:]), 10)
