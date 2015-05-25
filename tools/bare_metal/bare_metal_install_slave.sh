if [ "$1" == "" ]; then
    echo "First argument is missing, it has to be the hostname of the master machine."
    exit 1
fi
MASTER_HOSTNAME=$1

set -e
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
sudo sed -i 's/server_host=.*/server_host='"$MASTER_HOSTNAME"'/' /opt/cm-5.3.3/etc/cloudera-scm-agent/config.ini

# Create parcel directories
sudo mkdir -p /opt/cloudera/parcels

# Config the services to start automatically
sudo cp /opt/cm-5.3.3/etc/init.d/cloudera-scm-agent /etc/init.d/cloudera-scm-agent
sudo sed -i 's/CMF_DEFAULTS=.*/CMF_DEFAULTS=\/opt\/cm-5.3.3\/etc\/default/' /etc/init.d/cloudera-scm-agent
sudo update-rc.d cloudera-scm-agent defaults
sudo service cloudera-scm-agent start
echo "Cloudera installed successfully."

