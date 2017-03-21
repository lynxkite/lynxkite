#!/usr/bin/python3

import boto3
from prettytable import PrettyTable
import argparse

class MyTable():
        def __init__(self):
                self.attributes =  ['Region', 'Name', 'Type', 'State', 'Private IP', 'Public IP', 'Launch Time', 'Owner', 'Expiry Date']
                self.table = PrettyTable(self.attributes)
                self.table.align = 'l'

        def add_row(self, instance):
                self.table.add_row(instance)

        def get_table(self):
                return self.table

class MyEc2():
        def __init__(self):
                self.num_instance = 0
                self.client = boto3.client('ec2')
                self.regions = self.client.describe_regions()['Regions']
                self.table = MyTable()

        def get_running_instances(self):
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
                                        if 'Name' in tag['Key']:
                                                name = tag['Value']
                                        if 'owner' in tag['Key']:
                                                owner = tag['Value']
                                        if 'expiry' in tag['Key']:
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
                                self.num_instance += 1
                            
                                if len(instanceinfo) > 0:
                                        self.table.add_row(instanceinfo)
        def print_instances_ascii(self):
                print (self.table.get_table())

        def print_instances_html(self):
                msg = '<html><body>'
                msg += self.table.get_table().get_html_string()
                msg += '</html></body>'
                return str(msg)

        def get_num_instances(self):
                return self.num_instance


class MySesEmail():
        def __init__(self, to, subject):
                self.to = to
                self.from_addr = None
                self.subject = subject
                self.html = None
                self._format = 'html'

        def html_body(self, html):
                self._html = html

        def send(self, from_address=None):
                body = self._html

                if isinstance (self.to, str):
                        self.to = [self.to]
                if not from_address:
                        self.from_addr = 'ec2-users@lynxanalytics.com'
                if not self._html:
                        raise Exception('You must provide a html body')
                
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
        my_ec2 = MyEc2()
        my_ec2.get_running_instances()

        my_parser = argparse.ArgumentParser(description='Sends report of active AWS instances')
        my_parser.add_argument("--ascii", help="Prints instances in ascii table format", required=False, action="store_true")
        args = my_parser.parse_args()
        
        if args.ascii:
                my_ec2.print_instances_ascii()
        else:
                email = MySesEmail(to='ec2-users@lynxanalytics.com', subject='AWS Instances Report - %s instances running' % my_ec2.get_num_instances())
                email.html_body('%s' % my_ec2.print_instances_html())
                email.send()

