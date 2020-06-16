import lynx.kite
import lynx.automation
import pendulum
from lynx.automation import UTC, Schedule
from lynx.kite import text, pp, subworkspace, SideEffectCollector
from common import WORKSPACE_START_DATE
import psycopg2

# Set up database connection
# ==========================
DB_HOST = 'localhost'
DB_USER = 'kite'
DB_PASSWORD = 'kite'
DB_DB = 'kite'
BUSES_TABLE = 'buses'
STOPS_TABLE = 'stops'

JDBC_URL = f'jdbc:postgresql://{DB_HOST}:5432/{DB_DB}?user={DB_USER}&password={DB_PASSWORD}'

with psycopg2.connect(host=DB_HOST, user=DB_USER, password=DB_PASSWORD, dbname=DB_DB) as conn:
  try:
    print("Trying to create tables ...")
    cur = conn.cursor()
    cur.execute(f'CREATE TABLE {BUSES_TABLE} (timestamp timestamptz, reg_num varchar(20), centrality real, page_rank real)')
    cur.execute(f'CREATE TABLE {STOPS_TABLE} (timestamp timestamptz, stop_id varchar(20), centrality real, page_rank real)')
    print("... and tables created")
  except psycopg2.errors.DuplicateTable:
    print("... but they already exist")


# Recipe to import from parquet files in the datalake
# ===================================================
class ParquetFilesRecipe(lynx.automation.InputRecipe):
  def __init__(self, lk, base_prefixed_path, date_pattern='%Y-%m-%d %H-%M', success_file='_SUCCESS'):
    self.lk = lk
    self.base_prefixed_path = base_prefixed_path
    self.date_pattern = date_pattern
    self.success_file = success_file

  def directory_for_date(self, date):
    return f'{self.base_prefixed_path}/{date.format(self.date_pattern)}'

  def is_ready(self, date):
    resolved = self.lk.get_prefixed_path(f'{self.directory_for_date(date)}/{self.success_file}')
    return resolved.exists

  def build_boxes(self, date):
    return self.lk.importParquetNow(filename=f'{self.directory_for_date(date)}/*.parquet')

# Set up LynxKite connection
# ==========================
lk = lynx.kite.LynxKite()

# Definition of the workflow we want to do in each epoch
# ======================================================
@subworkspace
def create_graph(schedules, stops, sc=SideEffectCollector.AUTO):
  schedule_graph = schedules.useTableAsGraph(src='RN', dst='BUS_STOP_CD')
  with_id_attributes = (schedule_graph
      .aggregateEdgeAttributeToVertices(aggregate_BUS_STOP_CD='majority_100')
      .aggregateEdgeAttributeToVertices(direction='outgoing edges', aggregate_RN='majority_100')
      .renameVertexAttributes(
          change_edge_BUS_STOP_CD_majority_100='BUS_STOP', change_edge_RN_majority_100='RN')
      .deriveVertexAttribute(
          defined_attrs='false', output='type', expr='if (RN.isEmpty) \"Stop\" else \"Bus\"'))
  res = lk.useTableAsVertexAttributes(
      with_id_attributes, stops, id_attr='BUS_STOP', id_column='BUS_STOP_CD')
  sc.compute(res, "Create graph")
  return res

def compute_centrality(graph):
  return graph.computeCentrality(direction='all edges')

def compute_page_rank(graph):
  return graph.computePageRank(direction='all edges', damping=0.8)

@subworkspace
def prepare_for_visualization(graph):
  return (graph
      .aggregateOnNeighbors(direction='outgoing edges', aggregate_LAT='average', aggregate_LON='average')
      .mergeTwoVertexAttributes(name='LAT', attr1='LAT', attr2='neighborhood_LAT_average')
      .mergeTwoVertexAttributes(name='LON', attr1='LON', attr2='neighborhood_LON_average')
      .convertVertexAttributesToPosition(x='LAT', y='LON', output='GEO position')
      .filterByAttributes(filterva_LAT='<10000'))

def append_to_table(sc, table_name, table_data):
  return table_data.exportToJDBC(
      jdbc_url=JDBC_URL, jdbc_table=table_name, mode='Insert into an existing table', _id=f'Export to {table_name}').register(sc)

@lk.workspace_with_side_effects(parameters=[text('date')])
def centrality_pipeline(sc, schedules, stops):
  graph = create_graph(schedules, stops).register(sc)
  ce = sc.compute(compute_centrality(graph), "Compute centrality")
  pr = sc.compute(compute_page_rank(graph), "Compute page rank")
  combined = lk.projectRejoin(ce, pr, attrs='page_rank')

  append_to_table(
      sc,
      STOPS_TABLE,
      combined.sql(pp(
          '''select
               to_timestamp("$date") as timestamp,
               BUS_STOP as stop_id,
               centrality,
               page_rank
             from vertices
             where type="Stop"''')))
  append_to_table(
      sc,
      BUSES_TABLE,
      combined.sql(pp(
          '''select
               to_timestamp("$date") as timestamp,
               RN as reg_num,
               centrality,
               page_rank
             from vertices
             where type="Bus"''')))

  return {'graph': prepare_for_visualization(graph)}

# Set up automation for the above defined workflow
# ================================================
ws = lynx.automation.WorkspaceSequence(
    ws=centrality_pipeline,
    schedule=Schedule(
      WORKSPACE_START_DATE.add(minutes = 5),  # We want to start one cycle later than the data generation.
      '*/5 * * * *'),
    lk_root='centrality_pipeline',
    input_recipes=[
        ParquetFilesRecipe(lk, 'DATALAKE$/busdata/schedules/'),
        ParquetFilesRecipe(lk, 'DATALAKE$/busdata/stops/')])

airflow_dag = ws.to_airflow_DAG('centrality')
