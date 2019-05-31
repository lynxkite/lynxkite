import lynx.automation
from datetime import datetime

# No input, constant automatic output


def get_components_wss(lk):
  @lk.workspace()
  def components():
    graph = lk.createVertices(size=1000).createRandomEdges(degree=2, seed=12345)
    component_metrics = graph.findConnectedComponents().sql('''
          select
          "today" as date_id,
          max(size) as max_size,
          min(size) as min_size,
          count(*) as num_components
          from `connected_components.vertices`''')
    return dict(metrics=component_metrics)

  return lynx.automation.WorkspaceSequence(
      ws=components,
      schedule='30 * * * *',
      start_date=datetime(2018, 7, 13),
      lk_root='tedx_components',
      input_recipes=[]
  )
