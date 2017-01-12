'''Looks up Shapefile data by lat/long coordinates.

    pip3 install fiona pandas shapely
    python3 latlon_lookup.py
'''

import fiona
import pandas as pd
import shapely.geometry as sg

with fiona.open('my_file.shp') as f:
  shapes = list(f)

sps = [(sg.asShape(s['geometry']), s['properties']) for s in shapes]
locs = pd.read_csv('lat-lons.csv')


def lookup(id, lat, lon):
  point = sg.Point(lon, lat)
  d = {'id': id}
  hits = [p for (s, p) in sps if s.contains(point)]
  if hits:
    d.update(hits[0])
  return d

mapped = [lookup(id, lat, lon) for (idx, id, lat, lon) in locs.itertuples()]
mapped = pd.DataFrame(mapped)
mapped.to_csv('lat-lons-resolved.csv', index=False)
