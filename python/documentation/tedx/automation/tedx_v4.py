import lynx.automation
from datetime import datetime
from lynx.kite import text, pp

# Workspace with side effects.


def get_export_results_wss(input_folder, output_folder, lk):
  @lk.workspace_with_side_effects(parameters=[text('date')])
  def components(sec_collector, table):
    graph = table.useTableAsGraph(src='src', dst='dst')
    component_metrics = graph.findConnectedComponents().sql(pp('''
          select
          "$date" as date_id,
          max(size) as max_size,
          min(size) as min_size,
          count(*) as num_components
          from `connected_components.vertices`'''))
    exp = component_metrics.exportToParquet(path=pp(output_folder + '/${date}'))
    exp.register(sec_collector)
    return dict(metrics=component_metrics)

  return lynx.automation.WorkspaceSequence(
      ws=components,
      schedule='30 * * * *',
      start_date=datetime(2018, 7, 13),
      lk_root='tedx_export_results',
      input_recipes=[CSVRecipe(input_folder, lk)]
  )


class CSVRecipe(lynx.automation.InputRecipe):
  def __init__(self, local_folder, lk):
    self.local_folder = local_folder
    self.lk = lk

  def full_path(self, date):
    return self.local_folder + '/' + date.strftime("%Y-%m-%d-%H-%M") + '.csv'

  def marker(self, date):
    return self.local_folder + '/' + date.strftime("%Y-%m-%d-%H-%M") + '.SUCCESS'

  def is_ready(self, date):
    import os.path
    return os.path.isfile(self.marker(date))

  def build_boxes(self, date):
    prefixed_path = self.lk.upload(open(self.full_path(date)))
    return self.lk.importCSVNow(filename=prefixed_path)
