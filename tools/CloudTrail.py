import json
import boto3
import gzip
import shutil
import os
from datetime import datetime

region_list = {
    "us-east-2": "US East (Ohio)",
    "us-east-1": "US East (N. Virginia)",
    "us-west-1": "US West (N. California)",
    "us-west-2": "US West (Oregon)",
    "ap-south-1": "Asia Pacific (Mumbai)",
    "ap-northeast-2": "Asia Pacific (Seoul)",
    "ap-northeast-3": "Asia Pacific (Osaka-Local)",
    "ap-southeast-1": "Asia Pacific (Singapore)",
    "ap-southeast-2": "Asia Pacific (Sydney)",
    "ap-northeast-1": "Asia Pacific (Tokyo)",
    "ca-central-1": "Canada (Central)",
    "cn-north-1": "China (Beijing)",
    "eu-central-1": "EU (Frankfurt)",
    "eu-west-1": "EU (Ireland)",
    "eu-west-2": "EU (London)",
    "eu-west-3": "EU (Paris)",
    "sa-east-1": "South America (Sao Paulo)"
}


def reading_s3_cloudtrail(region='us-east-1'):
  s3 = boto3.resource("s3")
  bucket_name = "lynx-cloudtrail"
  bucket = s3.Bucket(bucket_name)
  today = datetime.now().strftime('%Y/%m/%d')
  for object in bucket.objects.filter(
          Prefix="AWSLogs/122496820890/CloudTrail/{}/{}/".format(region, today)):
    if object.key.endswith("gz"):
      key_name = object.key
      gzip_file_name = key_name.split('/')[-1]
      json_file_name = gzip_file_name.split('.')[0] + ".json"

      print("Downloading S3://{}/{} ...".format(bucket_name, key_name))
      bucket.download_file(key_name, "tmpfile.gz")
      with gzip.open("tmpfile.gz") as zipfile, open(json_file_name, 'wb') as inputfile:
        shutil.copyfileobj(zipfile, inputfile)
        os.remove("tmpfile.gz")
      parse_s3_cloudtrail(json_file_name)


def parse_s3_cloudtrail(json_file_name):
  print("Parsing {} ...".format(json_file_name))
  with open(json_file_name) as json_file:
    json_data = json.load(json_file)
    for record in json_data['Records']:
      print("  * {}:{} {} ({})".format(
          record.get("eventTime"),
          record.get("eventName"),
          record.get("userAgent"),
          record.get("userIdentity").get("arn", "na")
      ))
  os.remove(json_file_name)


if __name__ == "__main__":
  for region, region_name in region_list.items():
    print()
    print(" -- ({}) Region: {} {}--".format(
        datetime.now().strftime('%Y/%m/%d'),
        region,
        region_name))
    reading_s3_cloudtrail(region)
