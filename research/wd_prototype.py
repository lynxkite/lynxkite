import prompts
from pprint import pprint
import httpx
import os
import openai
import shelve
import SPARQLWrapper
import sys
import pandas as pd

pd.options.display.max_columns = None
shelf = shelve.open("wd.cache")
openai.api_key = os.getenv("OPENAI_API_KEY")
sparql = SPARQLWrapper.SPARQLWrapper('https://query.wikidata.org/sparql')
sparql.setReturnFormat(SPARQLWrapper.JSON)


def openai_text(prompt, **kwargs):
  key = prompt + repr(kwargs)
  if key not in shelf:
    response = openai.Completion.create(
        model="text-davinci-003",
        prompt=prompt,
        temperature=0,
        max_tokens=500,
        **kwargs,
    )
    shelf[key] = response.choices[0].text
  return shelf[key]


def openai_code(prompt, **kwargs):
  key = prompt + repr(kwargs)
  if key not in shelf:
    response = openai.Completion.create(
        model="code-davinci-002",
        prompt=prompt,
        temperature=0,
        max_tokens=500,
        **kwargs,
    )
    shelf[key] = response.choices[0].text
  return shelf[key]


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


def extract_entities(nodes, edge_condition):
  text = openai_text(
      prompts.ENTITY.replace("NODES", nodes).replace(
          "EDGE_CONDITION", edge_condition
      ),
      stop="----",
  )
  return [[x.strip() for x in xs.split("\n-")] for xs in text.split("Properties:\n-")]


def run_sparkql(query):
  if query not in shelf:
    sparql.setQuery(query)
    shelf[query] = sparql.queryAndConvert()
  data = []
  for row in shelf[query]["results"]["bindings"]:
    data.append({k: v['value'] for k, v in row.items()})
  return pd.DataFrame(data)


def get_graph(nodes, edge_condition, limit):
  entities, properties = extract_entities(nodes, edge_condition)
  print(entities)
  print(properties)
  schema = [(e, 'wd:' + search("item", e)) for e in entities] + [
      (e, 'wdt:' + search("property", e)) for e in properties
  ]
  schema_formatted = "\n".join(f"# - {e}: {id}" for (e, id) in schema)
  print(schema_formatted)
  sparql = openai_code(
      prompts.SPARQL.replace("NODES", nodes)
      .replace("EDGE_CONDITION", edge_condition)
      .replace("SCHEMA", schema_formatted),
      stop="####",
  )
  print(sparql)
  df = run_sparkql(f'SELECT {sparql}' + (f'\nLIMIT {limit}' if limit else ''))
  print(df)


def easy_get_graph(query, limit):
  text = openai_text(prompts.SPLIT_QUERY.replace('QUERY', query))
  [nodes, edge_condition] = [t.strip() for t in text.split('Connected: if')]
  print([nodes, edge_condition])
  return get_graph(nodes, edge_condition, limit)


# get_graph("defense industry companies", "one is a subsidiary of the other")
# easy_get_graph("Disney characters by movies", 10)
easy_get_graph(' '.join(sys.argv[1:]), 10)
