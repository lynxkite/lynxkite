#!/usr/bin/env python3
'''

This program reads the big AWS cost description file (index.json) from stdin,
converts it into a small json file that can readily be used by the instance reporter,
and dumps it to stdout.
The input file can be retrieved from here:
https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonEC2/current/index.json

The output json file has this format:
...
{
  "us-west-1@i3.4xlarge": "1.3760000000",
  "us-west-2@r3.large": "0.1660000000",
...
}

Example usage:
./gen_cost_dic.py < ~/Downloads/index.json > prices.json

'''
import json
import sys

cost_json = sys.stdin.read()

costs = json.loads(cost_json)

onDemand = costs['terms']['OnDemand']
products = costs['products']


REGION_2_CODE = {
    'US East (Ohio)': 'us-east-2',
    'US East (N. Virginia)': 'us-east-1',
    'US West (N. California)': 'us-west-1',
    'US West (Oregon)': 'us-west-2',
    'Asia Pacific (Mumbai)': 'ap-south-1',
    'Asia Pacific (Seoul)': 'ap-northeast-2',
    'Asia Pacific (Singapore)': 'ap-southeast-1',
    'Asia Pacific (Sydney)': 'ap-southeast-2',
    'Asia Pacific (Tokyo)': 'ap-northeast-1',
    'Canada (Central)': 'ca-central-1',
    'EU (Frankfurt)': 'eu-central-1',
    'EU (Ireland)': 'eu-west-1',
    'EU (London)': 'eu-west-2',
    'South America (Sao Paulo)': 'sa-east-1',
}


def get_dic_value(d):
  return next(iter(d.values()))


def jsonable_key(str1, str2):
  'Hack to create a string key from what is basically a tuple'
  assert('@' not in str1)
  assert('@' not in str2)
  return str1 + '@' + str2

output = {}
for k in products.keys():
  v = products[k]
  if v['productFamily'] == 'Compute Instance':
    attr = v['attributes']
    if attr['operatingSystem'] == 'Linux' and attr['tenancy'] == 'Shared':
      location = attr['location']
      if location != 'AWS GovCloud (US)':
        loc_code = REGION_2_CODE[location]
        instance_type = attr['instanceType']
        key = jsonable_key(loc_code, instance_type)
        assert(key not in output)
        sku = v['sku']
        term = onDemand[sku]
        priceDimensions = get_dic_value(term)['priceDimensions']
        priceInfo = get_dic_value(priceDimensions)
        assert(priceInfo['unit'] == 'Hrs')
        price = priceInfo['pricePerUnit']
        usd = price['USD']
        output[key] = usd


print(json.dumps(output, indent=2, sort_keys=True))
