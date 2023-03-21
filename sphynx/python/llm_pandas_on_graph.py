'''Generates Pandas code for a graph using OpenAI.'''
import itertools
import numpy as np
import pandas as pd
import re
import sys
# Langchain is not on Conda yet. Run "pip install langchain".
# https://github.com/hwchase17/langchain/issues/1271
import langchain


examples = [
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
        query='goats that jump the highest',
        nodes=pd.DataFrame(dict(
            id=[7, 99, 8],
            species=['goat', 'sheep', 'donkey'],
            jump_height=[13.5, 64.1, 33.2],
        )),
        edges=pd.DataFrame(dict(
            src=[0, 1, 2],
            dst=[1, 2, 3],
        )),
        output_schema='id: str, jump_height: int',
        solution='''
  goats = nodes[nodes['species'] == 'goat']
  return goats.nlargest(10, 'jump_height')
        '''.strip(),
        expected_result=pd.DataFrame(dict(
            id=[7], species=['goat'], jump_height=[13.5],
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
'''.strip()

answer_template = '''
```python
def compute_from_graph(nodes, edges):
  """{query}"""
  {solution}
```
'''.strip()


def run_code(*, nodes, edges, code):
  scope = {'pd': pd}
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


check_examples(examples)


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


def pandas_on_graph(*, nodes, edges, query, output_schema):
  nodes = cleanup(nodes)
  edges = cleanup(edges)
  pd.options.display.max_rows = 3 if len(nodes.columns) > 5 or len(edges.columns) > 5 else 10
  pd.options.display.max_columns = 100
  pd.options.display.width = 1000

  messages = list(itertools.chain.from_iterable(
      [
          langchain.schema.HumanMessage(content=question_template.format(**e)),
          langchain.schema.AIMessage(content=answer_template.format(**e)),
      ]
      for e in examples))
  messages.append(langchain.schema.HumanMessage(
      content=question_template.format(nodes=nodes, edges=edges, query=query, output_schema=output_schema)))
  msg = openai(messages)
  df = run_code(nodes=nodes, edges=edges, code=get_code(msg.content))
  if matches_schema(df, output_schema):
    return df
  messages.append(msg)
  messages.append(
      langchain.schema.HumanMessage(
          content='Make sure the result has these columns: ' +
          output_schema))
  msg = openai(messages)
  return run_code(nodes=nodes, edges=edges, code=get_code(msg.content))


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


if __name__ == '__main__':
  d = dict(examples[0])
  print(pandas_on_graph(
      nodes=d['nodes'], edges=d['edges'],
      query=sys.argv[1] if len(sys.argv) > 1 else d['query'],
      output_schema=d['output_schema']))
