'''Puts recent visitor statistics from the try.lynxkite.com logs to a spreadsheet.

To run locally:
  GOOGLE_APPLICATION_CREDENTIALS=~/Downloads/big-graph-gc1-7f6cca7b834e.json python update_stats_spreadsheet.py 2020-05-22
'''
# pip install google-api-python-client google-auth-httplib2 google-auth-oauthlib
# pip install maxminddb-geolite2
import datetime as dt
from geolite2 import geolite2
from googleapiclient import discovery
from google.auth.transport.requests import Request
import json
import os
import pandas as pd
import pickle
import re
import subprocess
import sys


def getlogs(today, filters):
  tomorrow = today + dt.timedelta(days=1)
  logs = subprocess.run(['gcloud', 'beta', 'logging', 'read', f'''
    {filters}
    timestamp >= "{today:%Y-%m-%d}"
    timestamp < "{tomorrow:%Y-%m-%d}"
    ''', '--format', 'json'], check=True, stdout=subprocess.PIPE).stdout
  return json.loads(logs)


def parselogs(logs):
  USER = re.compile(r'(\d+\.\d+\.\d+\.\d+) (.*?) (GET|POST)')
  GETWORKSPACE = re.compile(r'getWorkspace \{"top": *"(.*?)"')
  WIZARDS = re.compile(r'In progress wizards/(.*?)/')
  ips = {}
  for j in logs:
    ts = j['receiveTimestamp'].split('T')[1].split('.')[0]
    msg = j['textPayload'] if 'textPayload' in j else j['jsonPayload']['message']
    user = USER.search(msg)
    if user:
      ip = user.group(1)
      if ip not in ips:
        ips[ip] = {'first': ts, 'last': ts, 'count': 1, 'workspaces': set(), 'user': set()}
      else:
        ips[ip]['first'] = min(ts, ips[ip]['first'])
        ips[ip]['last'] = max(ts, ips[ip]['last'])
        ips[ip]['count'] = ips[ip]['count'] + 1
      ips[ip]['user'].add(user.group(2))
      for ws in GETWORKSPACE.findall(msg):
        if 'In progress wizards' not in ws and 'custom_boxes' not in ws:
          ips[ip]['workspaces'].add(ws)
      for ws in WIZARDS.findall(msg):
        if 'custom_boxes' not in ws:
          ips[ip]['workspaces'].add(ws)
  r = geolite2.reader()
  for ip in ips:
    ips[ip]['ip'] = ip
    ips[ip]['workspaces'] = ', '.join(ips[ip]['workspaces'])
    ips[ip]['user'] = ', '.join(ips[ip]['user'] - {'(not logged in)'})
    c = r.get(ip)
    ips[ip]['country'] = c['country']['names']['en'] if c else '?'
    ips[ip]['latitude'] = c['location']['latitude'] if c else 0
    ips[ip]['longitude'] = c['location']['longitude'] if c else 0
  if ips:
    return pd.DataFrame(ips.values()).sort_values('first')
  else:
    return pd.DataFrame()


def updatesheet(sheet, instance, sessions, today):
  result = SHEETS.get(spreadsheetId=sheet, range=instance + '!A:ZZ').execute()
  rows = result.get('values', [])
  for i, r in enumerate(rows):
    if r[0] == f'{today:%Y-%m-%d}':
      break
  else:
    i += 1
  SHEETS.update(
      spreadsheetId=sheet,
      range=instance + '!A' + str(i + 1),
      valueInputOption='RAW',
      body={'values': sessions.values.tolist()},
  ).execute()


def updateday(today):
  for instance in sorted(FILTERS):
    print(instance)
    sessions = parselogs(getlogs(today, FILTERS[instance]))
    if not sessions.empty:
      sessions['day'] = f'{today:%Y-%m-%d}'
      sessions = sessions[['day', 'ip', 'user', 'first', 'last',
                           'count', 'country', 'latitude', 'longitude', 'workspaces']]
      pd.options.display.max_rows = 0
      pd.options.display.max_colwidth = 0
      print(sessions.to_string(index=False))
      updatesheet('1ERwlfibXg0VG7bqrIF2N17yE2e4WfWvx7Zbde8o7YS8', instance, sessions, today)


SHEETS = discovery.build('sheets', 'v4').spreadsheets().values()
FILTERS = {
    'try.lynxkite.com': r'''
      resource.type = "gce_instance"
      resource.labels.instance_id = "2101385701744296416"
      jsonPayload.message =~ "\d+\.\d+\.\d+\.\d+"
      ''',
    'demo.lynxkite.com': r'''
      resource.type = "k8s_container"
      resource.labels.project_id = "external-lynxkite"
      resource.labels.location = "us-central1-a"
      resource.labels.cluster_name = "us-flex"
      resource.labels.namespace_name = "cloud-lk"
      resource.labels.pod_name =~ "jupyter-"
      textPayload =~ "\d+\.\d+\.\d+\.\d+"
      ''',
}

if len(sys.argv) > 1:
  updateday(dt.datetime.strptime(sys.argv[1], '%Y-%m-%d'))
else:
  today = dt.datetime.today()
  updateday(today - dt.timedelta(days=1))
  updateday(today)
