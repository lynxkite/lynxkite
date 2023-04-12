'''Generates Pandas code for a graph using OpenAI.'''
import itertools
import numpy as np
import pandas as pd
import re
import sys
import langchain


default_examples = [
    dict(
        query='find the chef with the most friends',
        nodes=pd.DataFrame(dict(
            age=[10, 20, 30],
            name=['Amanda', 'Bob', 'Cecil'],
            job=['carpenter', 'taylor', 'chef'],
        )),
        edges=pd.DataFrame(dict(
            src=[0, 1, 2],
            dst=[1, 2, 3],
            relationship=['friends', 'enemies', 'friends'],
        )),
        output_schema='name: str, friend_count: int',
        solution='''
  chefs = nodes[nodes['job'] == 'chef']
  friendships = edges[edges['relationship'] == 'friends']
  chefs['friend_count'] = friendships.groupby('src').size()
  return chefs.nlargest(1, 'friend_count')
        '''.strip(),
        expected_result=pd.DataFrame(dict(
            age=[30], name=['Cecil'], job=['chef'], friend_count=[1],
        ), index=[2]),
    ),
    dict(
        query='longest running friendships',
        nodes=pd.DataFrame(dict(
            id=[7, 99, 8],
            species=['goat', 'sheep', 'donkey'],
            jump_height=[13.5, 64.1, 33.2],
        )),
        edges=pd.DataFrame(dict(
            src=[0, 1, 2],
            dst=[1, 2, 3],
            start_date=[1999, 2012, 2013],
        )),
        output_schema='animal_a: str, animal_b: str, friendship_years: int',
        solution='''
  current_date = 2023
  edges['friendship_years'] = current_date - edges['start_date']
  # Convert the IDs to string.
  edges['animal_a'] = edges['src'].astype(str)
  edges['animal_b'] = edges['dst'].astype(str)
  return edges.nlargest(10, 'friendship_years')
        '''.strip(),
        expected_result=pd.DataFrame(dict(
            src=[0, 1, 2],
            dst=[1, 2, 3],
            start_date=[1999, 2012, 2013],
            friendship_years=[24, 11, 10],
            animal_a=['0', '1', '2'],
            animal_b=['1', '2', '3'],
        )),
    ),
    dict(
        query='the average income of friends in the same city',
        nodes=pd.DataFrame(dict(
            name=['David', 'Elmer', 'Felix'],
            city=['London', 'Paris', 'London'],
            income=[100, 200, 300],
        )),
        edges=pd.DataFrame(dict(
            src=[0, 1, 2],
            dst=[2, 3, 0],
        )),
        output_schema='name: str, avg_friend_income_in_city: float',
        solution='''
  edges = edges.merge(nodes.add_suffix('_src'), left_on='src', right_index=True)
  edges = edges.merge(nodes.add_suffix('_dst'), left_on='dst', right_index=True)
  # Keep only friends that are in the same city.
  edges = edges[edges['city_src'] == edges['city_dst']]
  nodes['avg_friend_income_in_city'] = edges.groupby('src')['income_dst'].mean()
  return nodes
        '''.strip(),
        expected_result=pd.DataFrame(dict(
            name=['David', 'Elmer', 'Felix'],
            city=['London', 'Paris', 'London'],
            income=[100, 200, 300],
            avg_friend_income_in_city=[300.0, np.nan, 100.0],
        )),
    ),
    dict(
        query='the cities connected with the most roads',
        nodes=pd.DataFrame(dict(
            name=['London', 'Paris', 'New York'],
            population=[100, 200, 300],
        )),
        edges=pd.DataFrame(dict(
            src=[0, 1, 2],
            dst=[1, 0, 3],
            cost=[100, 200, 300],
        )),
        output_schema='city1: str, city2: str, num_roads: int',
        solution='''
  edges = edges.merge(nodes.add_suffix('_src'), left_on='src', right_index=True)
  edges = edges.merge(nodes.add_suffix('_dst'), left_on='dst', right_index=True)
  edges = edges.rename(columns={'name_src': 'city1', 'name_dst': 'city2'})
  return pd.DataFrame({'num_roads': edges.groupby(['city1', 'city2']).size()}).reset_index()
        '''.strip(),
        expected_result=pd.DataFrame(dict(
            city1=['London', 'Paris'],
            city2=['Paris', 'London'],
            num_roads=[1, 1],
        )),
    ),
    dict(
        query='which nodes have an edge to a dead node?',
        nodes=pd.DataFrame(dict(
            id=[11, 22, 33],
            dead=[0, 0, 1],
        )),
        edges=pd.DataFrame(dict(
            src=[0, 1, 2],
            dst=[1, 2, 3],
        )),
        output_schema='id: int, dead_id: int',
        solution='''
  edges = edges.merge(nodes.add_suffix('_src'), left_on='src', right_index=True)
  edges = edges.merge(nodes.add_suffix('_dst'), left_on='dst', right_index=True)
  # Discard edges to living nodes.
  edges = edges[edges['dead_dst'] == 1]
  return edges.rename(columns={'src': 'id', 'dst': 'dead_id'})
        '''.strip(),
        expected_result=pd.DataFrame(dict(
            id=[1], dead_id=[2], id_src=[22], dead_src=[0], id_dst=[33], dead_dst=[1],
        ), index=[1]),
    ),
]


question_template = '''
"nodes" is a Pandas DataFrame with the following columns:
{nodes}

"edges" is a Pandas DataFrame with the following columns:
{edges}

You need to write a function `compute_from_graph(nodes, edges)` for the following task:
- {query}

The function should return a DataFrame with columns: {output_schema}

You can only use Pandas and Numpy which are already imported as `pd` and `np`.
'''.strip()

variation_template = '''
Change `compute_from_graph(nodes, edges)` to perform the following task:
- {query}
'''.strip()

answer_template = '''
```python
def compute_from_graph(nodes, edges):
  """{query}"""
  {solution}
```
'''.strip()


def format_df(df):
  '''Each column is on a separate line.'''
  lines = []
  for c in df.columns:
    examples = ', '.join(str(x) for x in df[c].values[:3])
    lines.append(f'- {c}: {examples}')
  return '\n'.join(lines)


# Monkey-patch DataFrame.__str__ so it's used in all prompts.
pd.DataFrame.__str__ = format_df


def run_code(*, nodes, edges, code):
  scope = {'pd': pd, 'np': np}
  exec(compile(code, 'generated code', 'exec'), scope)
  return scope['compute_from_graph'](nodes, edges)


def get_code(s):
  m = re.search('```python(.*)```', s, re.S)
  assert m, f'Could not find the Python code in "{s}"'
  return m[1].strip()


def check_examples(examples):
  for e in examples:
    # Validate example.
    code = get_code(answer_template).replace('{solution}', '') + e['solution']
    res = run_code(nodes=e['nodes'], edges=e['edges'], code=code)
    assert (res.equals(e['expected_result'])), str(res)


check_examples(default_examples)


def openai(messages):
  for m in messages:
    print(m.content)
  print('waiting for OpenAI...')
  chat = langchain.chat_models.ChatOpenAI(temperature=0)
  response = chat(messages)
  print(response.content)
  return response


def cleanup(df):
  '''Make sure the DataFrame can be used based on its printed form.'''
  for c in df.columns:
    df = df.rename(columns={c: c.strip()})
  return df


def human_msg(content):
  return langchain.schema.HumanMessage(content=content)


def ai_msg(content):
  return langchain.schema.AIMessage(content=content)


def pandas_on_graph(*, nodes, edges, query, output_schema, examples=None):
  nodes = cleanup(nodes)
  edges = cleanup(edges)
  pd.options.display.max_rows = 3 if len(nodes.columns) > 5 or len(edges.columns) > 5 else 10
  pd.options.display.max_columns = 100
  pd.options.display.width = 1000

  if examples:
    # Use the provided examples. They have the same data and schema, so we
    # only include them in the first message.
    messages = [
        human_msg(question_template.format(nodes=nodes, edges=edges,
                  query=examples[0][0], output_schema=output_schema)),
        ai_msg(answer_template.format(query=examples[0][0], solution=examples[0][1].strip())),
        *itertools.chain.from_iterable(
            [
                human_msg(variation_template.format(query=e[0])),
                ai_msg(answer_template.format(query=e[0], solution=e[1].strip())),
            ]
            for e in examples[1:]),
        human_msg(variation_template.format(query=query)),
    ]
  else:
    # Use the default examples. Include the full prompt each time because they
    # have different data and schemas.
    messages = [
        *itertools.chain.from_iterable(
            [
                human_msg(question_template.format(**e)),
                ai_msg(answer_template.format(**e)),
            ]
            for e in default_examples),
        human_msg(question_template.format(nodes=nodes, edges=edges,
                                           query=query, output_schema=output_schema))
    ]
  iterations = 3
  for i in range(iterations):
    msg = openai(messages)
    try:
      df = run_code(nodes=nodes, edges=edges, code=get_code(msg.content))
    except ImportError:
      messages.append(msg)
      messages.append(human_msg(f'''
Please do it without importing anything. You can use Pandas and Numpy. They are already
imported as `pd` and `np`. And maybe you can use an existing column instead of computing
something yourself.

"nodes" is a Pandas DataFrame with the following columns:
{nodes}

"edges" is a Pandas DataFrame with the following columns:
{edges}
        '''.strip()))
      continue
    except BaseException as exception:
      messages.append(msg)
      messages.append(human_msg(f'''
I'm getting an exception:
{exception}
        '''.strip()))
      continue
    if not matches_schema(df, output_schema):
      messages.append(msg)
      messages.append(human_msg('Make sure the result has these columns: ' + output_schema))
      continue
    # Success!
    return df


def matches_schema(df, schema):
  for col in schema.split(','):
    [name, type] = [x.strip() for x in col.split(':')]
    if name not in df:
      return False
    if type == 'str':
      type = 'O'
    if df[name].dtype != type:
      return False
  return True


def parse_examples(examples):
  examples = examples.strip().replace('\t', '  ')
  if not examples:
    return None
  lines = examples.split('\n')
  parsed = []
  for line in lines:
    line = line.rstrip()
    if not line:
      continue
    if line.startswith('  '):
      parsed[-1][1] += line + '\n'
    else:
      parsed.append([line.rstrip(':'), ''])
  return parsed


if __name__ == '__main__':
  d = dict(default_examples[0])
  print(pandas_on_graph(
      nodes=d['nodes'], edges=d['edges'],
      query=sys.argv[1] if len(sys.argv) > 1 else d['query'],
      output_schema='name: str, count: int',
      examples=parse_examples('''
find the chef with the most friends:
  nodes = nodes[nodes['job'] == 'chef']
  edges = edges[edges['relationship'] == 'friends']
  nodes['count'] = edges.groupby('src').size()
  return nodes.nlargest(1, 'count')

find the violinist with the least enemies:
  nodes = nodes[nodes['job'] == 'violinist']
  edges = edges[edges['relationship'] == 'enemies']
  nodes['count'] = edges.groupby('src').size()
  return nodes.nsmallest(1, 'count')

the three athletes that have the most fans:
  nodes = nodes[nodes['job'] == 'athlete']
  edges = edges[edges['relationship'] == 'fan']
  nodes['count'] = edges.groupby('dst').size()
  return nodes.nlargest(3, 'count')
     '''),
  ))
