#!/usr/bin/python3

import boto3
from prettytable import PrettyTable
import argparse


class ReportTable():

    def __init__(self):
        self.rows = 0
        self.attributes = [
            'Region',
            'Name',
            'Type',
            'State',
            'Private IP',
            'Public IP',
            'Launch Time',
            'Owner',
            'Expiry Date']
        self.table = PrettyTable(self.attributes)
        self.table.align = 'l'

    def add_row(self, instance):
        self.rows += 1
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
        return self.rows


def get_running_instances():
    table = ReportTable()
    client = boto3.client('ec2')
    regions = client.describe_regions()['Regions']
    for region in regions:
        region_name = region['RegionName']
        ec2 = boto3.resource('ec2', region_name=region['RegionName'])
        # Get information for all running instances
        running_instances = ec2.instances.filter(Filters=[{
                                                          'Name':
                                                              'instance-state-name',
                                                          'Values': ['running']}])
        for instance in running_instances:
            instanceinfo = []
            tags = {t['Key'].lower(): t['Value'] for t in instance.tags}
            instanceinfo.append(region_name.upper())
            instanceinfo.append(tags.get('name', 'undefined'))
            instanceinfo.append(instance.instance_type.upper())
            instanceinfo.append(instance.state['Name'].upper())
            instanceinfo.append(instance.private_ip_address)
            instanceinfo.append(instance.public_ip_address)
            instanceinfo.append(instance.launch_time)
            instanceinfo.append(tags.get('owner', 'undefined'))
            instanceinfo.append(tags.get('expiry', 'undefined'))

            table.add_row(instanceinfo)
    return table


def send_ses_email(
        to_address=None, from_address=None, subject=None, html_body=None):

    email_client = boto3.client('ses')
    email_info = {"to_address": "ec2-users@lynxanalytics.com",
                  "from_address": "ec2-users@lynxanalytics.com",
                  "subject": "Not filled subject",
                  "html_body": None}

    if to_address:
        email_info["to_address"] = [to_address]

    if from_address:
        email_info["from_address"] = from_address

    if subject:
        email_info["subject"] = subject

    if not html_body:
        if not email_info["html_body"]:
            raise Exception('You must provide a html body')
    else:
        email_info["html_body"] = html_body

    return email_client.send_email(
        Source=email_info["from_address"],
        Destination={
            'ToAddresses': email_info["to_address"],
        },
        Message={
            'Subject': {
                'Data': email_info["subject"],
            },
          'Body': {
              'Html': {
                  'Data': email_info["html_body"],
              }
            },
        },
    )


if __name__ == '__main__':
    table = get_running_instances()

    parser = argparse.ArgumentParser(
        description='Sends report of active AWS instances')
    parser.add_argument(
        "--ascii",
        help="Prints instances in ascii table format",
     required=False,
     action="store_true")
    args = parser.parse_args()

    if args.ascii:
        table.print_table_ascii()
    else:
        send_ses_email(
            to_address="ec2-users@lynxanalytics.com",
            from_address="ec2-users@lynxanalytics.com",
            subject='AWS Instances Report - %s instances running' % table.get_num_rows(
            ),
            html_body=table.print_table_html())
