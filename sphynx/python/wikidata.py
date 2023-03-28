'''Utility functions that use WikiData. To be used from 'Compute in Python' boxes.'''
import dataclasses
import httpx
import itertools
from pprint import pprint
import shelve
import tqdm

# LynxKite may run this code multiple times, so we cache the results.
shelf = shelve.open('wd.cache')


def get(url, **kwargs):
  '''Sends a GET request to the given URL, caching the result. Returns a JSON object.'''
  key = 'get-' + repr((url, kwargs))
  if key not in shelf:
    res = httpx.get(url, params=kwargs)
    res = res.json()
    shelf[key] = res
  return shelf[key]


def api(action, **kwargs):
  '''Calls the WikiData MediaWiki API. Returns a JSON object.'''
  return get(
      'https://www.wikidata.org/w/api.php',  # Open this for docs.
      action=action,
      language='en',
      format='json',
      **kwargs,
  )


def search(q, type='item', limit=3):
  '''Search based on labels. Returns a list of entity IDs that match the query.'''
  res = api('wbsearchentities', search=q, type=type, limit=limit)
  if not res:
    raise ValueError(f'WikiData cannot find {q}')
  return [e['id'] for e in res['search']]


def query(q, limit=3):
  '''Search based on full text. Returns a list of entity IDs that match the query.'''
  res = api('query', list='search', srsearch=q, srsort='incoming_links_desc', srlimit=limit)
  ids = [e['title'] for e in res['query']['search']]
  return ids


def entity_data(entity_id):
  '''Returns the entity data (including all properties) for the entity.'''
  j = get(f'https://www.wikidata.org/wiki/Special:EntityData/{entity_id}.json')
  return j['entities'][entity_id]


@dataclasses.dataclass
class Entity:
  '''A simplified view of a WikiData entity.'''
  id: str
  properties: dict  # Property ID -> value or wikibase-entityid dict


def get_properties(entity_id):
  '''Returns a dictionary of properties for the entity. This is a simplified view of the entity data.'''
  j = entity_data(entity_id)
  claims = j['claims']
  props = {}
  # There can be multiple values for each property. We pick the most recent one. (Or the first one if there are no dates.)
  for p, vs in claims.items():
    besttime = None
    bestv = None
    for v in vs:
      if v['mainsnak']['snaktype'] != 'value' or v['mainsnak']['datatype'] not in ['quantity', 'monolingualtext', 'wikibase-item']:
        continue
      if bestv is None:
        bestv = v
      for q in v.get('qualifiers', {}).values():
        for t in q:
          if t['snaktype'] == 'value' and t['datavalue']['type'] == 'time':
            time = t['datavalue']['value']['time']
            if besttime is None or time > besttime:
              besttime = time
              bestv = v
    if bestv:
      v = bestv['mainsnak']['datavalue']
      if v['type'] == 'quantity':
        # TODO: Make sure they are all in the same unit.
        props[p] = float(v['value']['amount'])
      elif v['type'] == 'monolingualtext':
        props[p] = v['value']['text']
      elif v['type'] == 'string' or v['type'] == 'wikibase-entityid':
        props[p] = v['value']
      else:
        props[p] = v['value']
  return Entity(id=entity_id, properties=props)


def get_labels(ids):
  '''Gets the labels of a list of entities.'''
  remaining = []
  for id in ids:
    key = 'label-' + id
    if key in shelf:
      yield id, shelf[key]
    else:
      remaining.append(id)
  ids = remaining
  # The API limits us to 50 IDs at a time.
  for i in range(0, len(ids), 50):
    res = api('wbgetentities', ids='|'.join(ids[i:i+50]), props='labels')
    for id, v in res['entities'].items():
      label = v['labels']['en']['value']
      key = 'label-' + id
      shelf[key] = label
      yield id, label


def score_properties(eds):
  '''Returns a score for each property that reflects how much information it carries.'''
  sets = {}
  for e in eds:
    for p in e:
      v = e[p]
      if isinstance(v, dict):
        v = v['id']
      sets.setdefault(p, set()).add(v)
  return {k: len(v) for k, v in sets.items()}


def enrich(df, key_col, limit_props=10):
  '''
  Enriches a DataFrame with data from WikiData.

  Example use:

  df = pd.DataFrame({'city': ['New York', 'London', 'Paris']})
  df = enrich(df, 'city')
  '''
  keys = df[key_col].unique()
  eds = []
  # Look up each key and fetch the properties for all matches.
  for k in tqdm.tqdm(keys):
    ids = search(k)
    eds.append([get_properties(id) for id in ids])
  propscores = score_properties(e.properties for e in itertools.chain.from_iterable(eds))
  # Pick the entity with the most common properties for each key.
  for i, ed in enumerate(eds):
    eds[i] = max(ed, key=lambda e: sum(propscores[p] for p in e.properties))
  propscores = score_properties(e.properties for e in eds)
  bestprops = set(sorted(propscores, key=propscores.get, reverse=True)[:limit_props])
  # Narrow down the properties and look up labels.
  ids = [*bestprops]
  for e in eds:
    e.properties = {k: v for k, v in e.properties.items() if k in bestprops}
    ids.extend(v['id'] for p, v in e.properties.items() if isinstance(v, dict))
  labels = dict(get_labels(ids))
  # Add the labels.
  for e in eds:
    e.properties = {
        labels[k]: labels[v['id']] if isinstance(v, dict) else v
        for k, v in e.properties.items()}
  # Merge the data.
  for key, e in zip(keys, eds):
    e.properties['_key'] = key
    e.properties['wikidata_id'] = e.id
  eds = pd.DataFrame([e.properties for e in eds])
  return df.merge(eds, left_on=key_col, right_on='_key').drop('_key', axis=1)


if __name__ == '__main__':
  import pandas as pd
  pd.options.display.max_columns = 100
  pd.options.display.width = 1000
  df = pd.DataFrame({'name':  ['New York', 'London', 'Paris']})
  print()
  print(enrich(df, 'name'))
  df = pd.DataFrame({'name':  ['Keanu Reeves', 'Will Smith', 'Nicole Kidman', 'Sandra Bullock']})
  print()
  print(enrich(df, 'name'))
  df = pd.DataFrame({'name':  [
      'New York', 'London', 'Paris',
      'Keanu Reeves', 'Will Smith', 'Adam Sandler', 'Arnold Schwarzenegger', 'Nicole Kidman', 'Sandra Bullock']})
  print()
  print(enrich(df, 'name'))
  df = pd.DataFrame({'name':  ['Mississippi', 'California', 'Texas', 'New York', 'Florida', 'Illinois', 'Pennsylvania']})
  print()
  print(enrich(df, 'name'))
  df = pd.DataFrame({'name':  ['Mississippi', 'Amazon', 'Nile', 'Yangtze', 'Danube', 'Congo', 'Mekong']})
  print()
  print(enrich(df, 'name'))
  df = pd.DataFrame({'name':  ['Amazon', 'Google', 'Facebook', 'Microsoft']})
  print()
  print(enrich(df, 'name'))
