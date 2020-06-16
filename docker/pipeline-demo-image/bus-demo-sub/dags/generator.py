import lynx.kite
import lynx.automation
import pendulum
from lynx.automation import UTC, Schedule
from lynx.kite import text, pp
from common import WORKSPACE_START_DATE

lk = lynx.kite.LynxKite()
SCHEDULE_SNAPSHOT_NAME = "schedule"
STOPS_SNAPSHOT_NAME = "stops"

SCHEDULE_START_DATE = pendulum.create(2017, 4, 5, 8, 0, 0, tz=UTC)

def dformat(d):
  return d.format('%Y-%m-%d %H:%M:%S')

class ScheduleRecipe(lynx.automation.InputRecipe):
  def __init__(self, lk):
    self.lk = lk

  def is_ready(self, date):
    return date <= WORKSPACE_START_DATE.add(minutes=125)


  def build_boxes(self, date):
    schedule_entry = lk.get_directory_entry(SCHEDULE_SNAPSHOT_NAME)
    if schedule_entry.exists:
        assert schedule_entry.isSnapshot
    else:
        lk.importCSVNow(
            filename='DATALAKE$input_base/bus-schedules-8-dedup.csv',
            columns='ACTL_SVC_NUM,REG_NUM,BUS_STOP_CD,BUS_STOP_ARRVL_DTTM').save_snapshot(SCHEDULE_SNAPSHOT_NAME)

    all_data = lk.importSnapshot(path=SCHEDULE_SNAPSHOT_NAME)
    # We have one hour of schedule data, starting at SCHEDULE_START_DATE. What we want is to
    # push a 10 minutes time window over this data. We want to push the window with 2 minutes at a time. But we want to
    # run the DAG every five minutes, so that a complete run takes more than 2 hours. The below code performs the translation
    # between the date of the DAG to the correct actual location of the time window.
    start = SCHEDULE_START_DATE + (date - WORKSPACE_START_DATE) * 2 / 5
    end = start.add(minutes=10)
    start = dformat(start)
    end = dformat(end)
    return all_data.sql(f'''
        select *, CONCAT("REG_", REG_NUM) as RN from input
        where (BUS_STOP_ARRVL_DTTM > "{start}") AND
              (BUS_STOP_ARRVL_DTTM < "{end}")''')

class StopsRecipe(lynx.automation.InputRecipe):
  def __init__(self, lk):
    self.lk = lk

  def is_ready(self, date):
    return True


  def build_boxes(self, date):
    stops_entry = lk.get_directory_entry(STOPS_SNAPSHOT_NAME)
    if stops_entry.exists:
        assert stops_entry.isSnapshot
    else:
        raw = lk.importCSVNow(
            filename='DATALAKE$input_base/bus-stops.csv')
        filtered = raw.sql('''
            select
              BUS_STOP_CD,
              FIRST(BUS_STOP_NAM) as BUS_STOP_NAM,
              DOUBLE(FIRST(Latitude)) as LAT,
              DOUBLE(FIRST(Longitude)) as LON
            from input group by BUS_STOP_CD having count(*) = 1''')
        filtered.save_snapshot(STOPS_SNAPSHOT_NAME)

    return lk.importSnapshot(path=STOPS_SNAPSHOT_NAME)


@lk.workspace_with_side_effects(parameters=[text('date')])
def input_generation(sc, schedules, stops):
  reformatted_date = '''${
val dest_format = org.joda.time.format.DateTimeFormat.forPattern("yyyy-MM-dd HH-mm")
val src_format = org.joda.time.format.ISODateTimeFormat.dateTimeNoMillis()
dest_format.print(src_format.parseDateTime(date).plusMinutes(5))
}'''
  schedules.exportToParquet(path=pp(f'DATALAKE$$/busdata/schedules/{reformatted_date}')).register(sc)
  stops.exportToParquet(path=pp(f'DATALAKE$$/busdata/stops/{reformatted_date}')).register(sc)

ws = lynx.automation.WorkspaceSequence(
    ws=input_generation,
    schedule=Schedule(WORKSPACE_START_DATE, '*/5 * * * *'),
    lk_root='input_generation',
    input_recipes=[ScheduleRecipe(lk), StopsRecipe(lk)])

airflow_dag = ws.to_airflow_DAG('input_generation')
