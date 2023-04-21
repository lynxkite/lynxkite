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
  # There can be multiple values for each property. We pick the most recent
  # one. (Or the first one if there are no dates.)
  for p, vs in claims.items():
    besttime = None
    bestv = None
    for v in vs:
      if v['mainsnak']['snaktype'] != 'value' or v['mainsnak']['datatype'] not in [
              'quantity', 'monolingualtext', 'wikibase-item']:
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
    res = api('wbgetentities', ids='|'.join(ids[i:i + 50]), props='labels')
    for id, v in res['entities'].items():
      if 'en' not in v['labels']:
        continue
      label = v['labels']['en']['value']
      key = 'label-' + id
      shelf[key] = label
      yield id, label


def score_properties(eds):
  '''Returns a score for each property that reflects how much information it carries.'''
  sets = {}
  counts = {}
  for e in eds:
    for p in e:
      counts[p] = counts.get(p, 0) + 1
      v = e[p]
      if isinstance(v, dict):
        v = v['id']
      sets.setdefault(p, set()).add(v)
  return {k: counts[k] if len(sets[k]) > 1 else 0 for k in counts}


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
        for k, v in e.properties.items() if k in labels and (not isinstance(v, dict) or v['id'] in labels)}
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
  '''
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
  '''
  df = pd.DataFrame({'name': "Sweden;Montenegro;American Samoa;Isle of Man;France;Niger;Tuvalu;Suriname;Serbia;Mexico;Nepal;Paraguay;Iraq;Norfolk Island;Canada;Central African Republic;Egypt;Kiribati;Comoros;Turks and Caicos Islands;Oman;Portugal;Yemen;Sao Tome and Principe;East Timor;Djibouti;Ecuador;Germany;Monaco;Brazil;Luxembourg;Norway;Guernsey;Denmark;Estonia;Gabon;Guam;Nauru;South Korea;Slovenia;Czech Republic;Bolivia;Congo (Kinshasa);Guinea;Ghana;Nigeria;Chad;Barbados;Ethiopia;Bahrain;Cayman Islands;Saint Kitts and Nevis;China;Chile;Kenya;Kyrgyzstan;Cambodia;Lithuania;Finland;Antigua and Barbuda;Micronesia;Cyprus;Spain;Trinidad and Tobago;Singapore;Libya;Namibia;Bulgaria;Martinique;United States;Faroe Islands;Angola;Vanuatu;Zambia;French Guiana;Somalia;Tajikistan;Botswana;Tanzania;Malta;Uruguay;Bosnia and Herzegovina;Northern Mariana Islands;Madagascar;Nicaragua;Jordan;North Korea;Reunion;Christmas Island;El Salvador;Cape Verde;Dominica;Indonesia;Greece;Australia;Russia;Georgia;Ukraine;United Kingdom;Wallis and Futuna;Equatorial Guinea;Palau;Macedonia;Western Sahara;Bhutan;Netherlands;Laos;Lesotho;New Caledonia;Venezuela;Aruba;Netherlands Antilles;South Africa;Afghanistan;Poland;Gibraltar;Ireland;Kazakhstan;Saint Lucia;Congo (Brazzaville);Sierra Leone;Italy;Grenada;Azerbaijan;Uganda;Malaysia;Eritrea;Mozambique;Panama;Albania;Maldives;Hong Kong;Benin;Liberia;Japan;Saudi Arabia;Bangladesh;Mauritania;Romania;Virgin Islands;Burkina Faso;Moldova;Colombia;New Zealand;Iran;Greenland;Belarus;Macau;Cook Islands;Burma;Puerto Rico;Gambia;Costa Rica;Mali;Guatemala;Vietnam;Jamaica;Croatia;Honduras;Cote d'Ivoire;Kuwait;Saint Vincent and the Grenadines;Taiwan;Iceland;Anguilla;South Sudan;Seychelles;Switzerland;Turkmenistan;Belize;Slovakia;Cameroon;Sudan;Qatar;Sri Lanka;Tunisia;Solomon Islands;French Polynesia;Bermuda;Swaziland;Dominican Republic;Burundi;Mayotte;Jersey;Guyana;Thailand;Hungary;Belgium;Algeria;Zimbabwe;Tonga;Mauritius;Saint Pierre and Miquelon;Bahamas;Peru;Papua New Guinea;Uzbekistan;United Arab Emirates;Senegal;Samoa;Brunei;Morocco;India;Togo;Cuba;Marshall Islands;Guinea-Bissau;Austria;Argentina;Guadeloupe;Malawi;Haiti;Mongolia;Israel;Turkey;Philippines;British Virgin Islands;Cocos (Keeling) Islands;Pakistan;Fiji;Latvia;Falkland Islands;Armenia;Rwanda;Lebanon".split(';')})
  # df = df.head(5)
  df = enrich(df, 'name', limit_props=100)
  print(df)
  rates = {c: df[c].notnull().sum() / len(df) for c in df.columns}
  for c in sorted(df.columns, key=lambda c: rates[c], reverse=True):
    print(f'{c}: {rates[c]:.0%}')
    print('- ' + ', '.join(df[c].astype(str).unique()[:5]))
  df = pd.DataFrame({
      'country': df['name'],
      'country_area': df['area'],
      'country_form': df['instance of'],
      'currency': df['currency'],
      'continent': df['continent'],
      'language': df['official language'],
      'country_tld': df['top-level Internet domain'],
      'country_flag': df['short name'],
      'country_demonym': df['demonym'],
      'country_head_of_government': df['head of government'],
      'country_anthem': df['anthem'],
      'country_plug_type': df['electrical plug type'],
      'country_voltage': df['mains voltage'],
      'country_driving_side': df['driving side'],
      'country_member_of': df['member of'],
      'country_total_fertility_rate': df['total fertility rate'],
      'country_life_expectancy': df['life expectancy'],
      'country_part_of': df['part of'],
      'country_human_development_index': df['Human Development Index'],
  })
  df.to_parquet('country-from-wikidata.parquet', index=False)
