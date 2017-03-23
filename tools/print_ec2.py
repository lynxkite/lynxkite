#!/usr/bin/python3

import boto3
from prettytable import PrettyTable
import argparse

class ReportTable():
  def __init__(self):
    self.row = 0
    self.attributes =  ['Region', 'Name', 'Type', 'State', 'Private IP', 'Public IP', 'Launch Time', 'Owner', 'Expiry Date']
    self.table = PrettyTable(self.attributes)
    self.table.align = 'l'

  def add_row(self, instance):
    self.table.add_row(instance)

  def print_table_ascii(self):
    print ("%s running instances" % self.get_num_rows())
    print (self.table)

  def print_table_html(self):
    msg = "<html><body>"
    msg += self.table.get_html_string()
    msg += "</body></html>"
    return str(msg)

  def get_num_rows(self):
    return self.row


class Ec2():
  def __init__(self):
    self.client = boto3.client('ec2')
    self.regions = self.client.describe_regions()['Regions']

  def get_running_instances(self):
    table = ReportTable()
    for region in self.regions:
      region_name = region['RegionName']
      ec2 = boto3.resource('ec2', region_name=region['RegionName'])
      #Get information for all running instances
      running_instances = ec2.instances.filter(Filters=[{
                                'Name':'instance-state-name',
                                'Values': ['running']}])
      for instance in running_instances:
        instanceinfo = []
        for tag in instance.tags:
          if 'name' in tag['Key'].lower():
            name = tag['Value']
          if 'owner' in tag['Key'].lower():
            owner = tag['Value']
          if 'expiry' in tag['Key'].lower():
            expiry = tag['Value']
        instanceinfo.append(region_name.upper())
        instanceinfo.append(name)
        instanceinfo.append(instance.instance_type.upper())
        instanceinfo.append(instance.state['Name'].upper())
        instanceinfo.append(instance.private_ip_address)
        instanceinfo.append(instance.public_ip_address)
        instanceinfo.append(instance.launch_time)
        instanceinfo.append(owner)
        instanceinfo.append(expiry)
                            
        if len(instanceinfo) > 0:
          table.add_row(instanceinfo)
          table.row += 1
    return table

class SesEmail():
  def __init__(self):
    self.to = None
    self.from_addr = None
    self.subject = None
    self._html = None
    self._format = 'html'

  def html_body(self, html):
    self._html = html

  def send_ses_email(self, to_address=None, from_address=None, subject=None, html_body=None):
    if not to_address:
      self.to = ["ec2-users@lynxanalytics.com"]
    else:
      self.to = [to_address]
    
    if not subject:
      self.subject = "Not filled subject"
    else:
      self.subject = subject
    
    if not from_address:
      self.from_addr = 'ec2-users@lynxanalytics.com'
    else:
      self.from_addr = from_address
    
    if not html_body:
      if not self._html:
        raise Exception('You must provide a html body')
    else:
      self._html = html_body

    email_client = boto3.client('ses')
    
    return email_client.send_email(
                Source=self.from_addr,
                Destination={
                    'ToAddresses': self.to,
                            },
                Message={
                    'Subject': {
                    'Data': self.subject
                        },
                    'Body':{
                    'Html': {
                        'Data': self._html
                            }
                            },
                            },
                )


if __name__ == '__main__':
  my_ec2 = Ec2()
  my_table = my_ec2.get_running_instances()

  my_parser = argparse.ArgumentParser(description='Sends report of active AWS instances')
  my_parser.add_argument("--ascii", help="Prints instances in ascii table format", required=False, action="store_true")
  args = my_parser.parse_args()
        
  if args.ascii:
    my_table.print_table_ascii()
  else:
    email = SesEmail()
    email.send_ses_email(
        to_address="ec2-users@lynxanalytics.com",
        from_address="ec2-users@lynxanalytics.com",
        subject='AWS Instances Report - %s instances running' % my_table.get_num_rows(),
        html_body=my_table.print_table_html()
        )
