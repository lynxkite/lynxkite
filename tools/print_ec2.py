#!/usr/bin/python3

import boto3
from prettytable import PrettyTable


client = boto3.client('ec2')
regions = client.describe_regions()['Regions']
attributes = ['Region', 'Name', 'Type', 'State', 'Private IP', 'Public IP', 'Launch Time', 'Owner', 'Expiry Date']
table = PrettyTable(attributes) 
table.align = 'l'
for region in regions:
    region_name=region['RegionName']
    
    ec2 = boto3.resource('ec2',region_name=region['RegionName'])

    # Get information for all running instances
    running_instances = ec2.instances.filter(Filters=[{
    'Name': 'instance-state-name',
    'Values': ['running']}])

    
    for instance in running_instances:
        instanceinfo = []
        for tag in instance.tags:
            if 'Name'in tag['Key']:
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

        if len(instanceinfo) > 0:
            table.add_row(instanceinfo)

message = table.get_html_string()
print(message)



