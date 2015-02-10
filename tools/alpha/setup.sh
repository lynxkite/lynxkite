#!/bin/sh -xue

# Make sure we are not killing someone's laptop.
if [ $(whoami) != 'ec2-user' ]; then
  echo This script must be run on the EC2 instance.
  exit 1
fi

# Mount the SSD if it's not mounted yet.
if [ ! -e /media/ephemeral0/lost+found ]; then
  sudo mkfs.ext4 /dev/xvdb
  sudo mount /media/ephemeral0
  sudo chown ec2-user /media/ephemeral0
fi

# Redirect port 80 to port 9000, 443 to 9001.
sudo iptables -A PREROUTING -t nat -i eth0 -p tcp --dport 80 -j REDIRECT --to-port 9000
sudo iptables -A PREROUTING -t nat -i eth0 -p tcp --dport 443 -j REDIRECT --to-port 9001

# Start server.
biggraphstage/bin/biggraph restart
