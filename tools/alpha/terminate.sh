#!/bin/sh -xue

INSTANCE=$(aws ec2 describe-instances --filters Name=group-name,Values=alpha Name=instance-state-name,Values=running| grep INSTANCES | cut -f 8)
aws ec2 terminate-instances --instance-ids $INSTANCE
