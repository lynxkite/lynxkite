#!/usr/bin/env python3
""""

This program converts the on demand costs file into a small json file
that can readily be used by the instance reporter. Then the result
is dumped to stdout.

The output json file has this format:
...
{
  "us-west-1@i3.4xlarge": "1.3760000000",
  "us-west-2@r3.large": "0.1660000000",
...
}

Example usage:
./gen_cost_dic.py > prices.json

"""
import os
import sys
import json

# Set up import path for our modules.
os.chdir(os.path.dirname(__file__))
sys.path.append('../remote_api/python')

from utils.emr_lib import get_on_demand_costs_and_print_skipped_regions

costs = get_on_demand_costs_and_print_skipped_regions()


def jsonable_key(str1, str2):
  """Hack to create a string key from what is basically a tuple"""
  assert('@' not in str1)
  assert('@' not in str2)
  return str1 + '@' + str2

output = {}
for region_type in costs.keys():
  cost = costs[region_type]
  jkey = jsonable_key(region_type[0], region_type[1])
  output[jkey] = cost

print(json.dumps(output, indent=2, sort_keys=True))
