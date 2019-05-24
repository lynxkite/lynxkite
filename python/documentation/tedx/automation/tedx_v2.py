import lynx.automation
from datetime import datetime
from lynx.kite import text, pp

# No input, date dependent automatic output


def get_components_by_date_wss(lk):
  @lk.workspace(parameters=[text('date')])
  def components():
    graph = lk.createVertices(size=1000).createRandomEdges(degree=2, seed=pp('${date.hashCode()}'))
    component_metrics = graph.findConnectedComponents().sql(pp('''
          select
          "$date" as date_id,
          max(size) as max_size,
          min(size) as min_size,
          count(*) as num_components
          from `connected_components.vertices`'''))
    return dict(metrics=component_metrics)

  return lynx.automation.WorkspaceSequence(
      ws=components,
      schedule='30 * * * *',
      start_date=datetime(2018, 7, 13),
      lk_root='tedx_components_by_date',
      input_recipes=[]
  )
