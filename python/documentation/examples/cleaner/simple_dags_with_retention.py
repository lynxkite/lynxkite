'''Demo dags to test the automated cleaner task

Set KITE_CLEANER_MIN_AGE_DAYS to a positive value in .kiterc
to run this test.
'''


import lynx.kite
from lynx.kite import text, pp
import lynx.automation
from get_values import filename
from datetime import datetime, timedelta


lk = lynx.kite.LynxKite()


class CSVRecipe(lynx.automation.InputRecipe):
  def __init__(self, lk):
    self.lk = lk

  def is_ready(self, date):
    import os.path
    return os.path.isfile(filename('index', date))

  def build_boxes(self, date):
    prefixed_path = self.lk.upload(open(filename('index', date)))
    return self.lk.importCSVNow(filename=prefixed_path)


@lk.workspace(parameters=[text('date')])
def save(table):
  table_with_timestamp = table.sql(pp('''
    select
    "$date" as date_id,
    cast(supporters as integer) as supporters,
    cast(amount as integer) as amount
    from input'''))
  return dict(hourly=table_with_timestamp)


save_wss = lynx.automation.WorkspaceSequence(
    ws=save,
    schedule='0 * * * *',
    start_date=datetime(2018, 11, 21, 12, 0),
    lk_root='index_campaign_data',
    input_recipes=[CSVRecipe(lk)]
)


class SnapshotUnionRecipe(lynx.automation.InputRecipe):
  def __init__(self, tss, start_date):
    self.tss = tss
    self.start_date = start_date

  def is_ready(self, date):
    from croniter import croniter
    tss_recipe = lynx.automation.TableSnapshotRecipe(self.tss)
    i = croniter(self.tss.cron_str, self.start_date - timedelta(seconds=1))
    t = []
    while True:
      dt = i.get_next(datetime)
      if dt > date:
        break
      if not tss_recipe.is_ready(dt):
        return False
    return True

  def build_boxes(self, date):
    return self.tss.read_interval(self.start_date, date)


snapshot_union_recipe = SnapshotUnionRecipe(
    save_wss.output_sequences['hourly'], datetime(
        2018, 11, 21, 12, 0))


@lk.workspace()
def aggregate(table):
  s_aggr = (table
            .sql('select date(date_id) as date, supporters from input')
            .sql('select date, max(supporters)-min(supporters) as s_inc from input group by date'))
  a_aggr = (table
            .sql('select date(date_id) as date, amount from input')
            .sql('select date, max(amount)-min(amount) as a_inc from input group by date'))
  current = table.sql('select max(supporters) as s, max(amount) as a from input')
  return dict(supporters=s_aggr, amount=a_aggr, current=current)


analysis_wss = lynx.automation.WorkspaceSequence(
    ws=aggregate,
    schedule='0 * * * *',
    start_date=datetime(2018, 11, 21, 12, 0),
    lk_root='index_campaign_analysis',
    input_recipes=[snapshot_union_recipe],
    default_retention=timedelta(days=1)
)

save_dag = save_wss.to_airflow_DAG('data-sequence-creator')
analysis_dag = analysis_wss.to_airflow_DAG('aggregations')
