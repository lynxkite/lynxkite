echo "Starting LynxKite installation..."

DVD_ROOT="$(dirname $0)"

# Install Java
echo "Installing Java..."
sudo mkdir -p /usr/java
sudo tar -xf $DVD_ROOT/server-jre-7u80-linux-x64.tar.gz -C /usr/java
echo "Java installed successfully."

# Install Cloudera
echo "Installing Cloudera..."
sudo tar -xf $DVD_ROOT/cloudera-manager-trusty-cm5.3.3_amd64.tar.gz -C /opt

# Config the server host
echo "Please type the private IPv4 address of the master machine:"
read MASTER_IP
sudo sed -i 's/server_host=.*/server_host='"$MASTER_IP"'/' /opt/cm-5.3.3/etc/cloudera-scm-agent/config.ini

# Create parcel directories
sudo mkdir -p /opt/cloudera/parcels

# Config the services to start automatically
sudo cp /opt/cm-5.3.3/etc/init.d/cloudera-scm-agent /etc/init.d/cloudera-scm-agent
sudo sed -i 's/CMF_DEFAULTS=.*/CMF_DEFAULTS=\/opt\/cm-5.3.3\/etc\/default/' /etc/init.d/cloudera-scm-agent
sudo update-rc.d cloudera-scm-agent defaults
sudo service cloudera-scm-agent start
echo "Cloudera installed successfully."

