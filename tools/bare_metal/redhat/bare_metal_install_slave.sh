if [ "$#" -ne 1 ]; then
    >&2 echo "Usage: $0 <hostname of the master machine>."
    exit 1
fi
MASTER_HOSTNAME=$1

set -ue
echo "Starting LynxKite installation..."

DVD_ROOT="$(dirname $0)"

# Install Java
echo "Installing Java..."
sudo mkdir -p /usr/java
sudo tar -xf $DVD_ROOT/server-jre-7u80-linux-x64.tar.gz -C /usr/java
sudo sh -c "echo export JAVA_HOME=/usr/java/jdk1.7.0_80/ > /etc/profile.d/lynx.sh"
echo "Java installed successfully."

# Install Cloudera
echo "Installing Cloudera..."
sudo tar -xf $DVD_ROOT/cloudera-manager-el6-cm5.4.7_x86_64.tar.gz -C /opt
sudo useradd --system --home=/opt/cm-5.4.7/run/cloudera-scm-server --no-create-home cloudera-scm
sudo chown -R cloudera-scm.cloudera-scm /opt/cm-5.4.7

# Config the server host
sudo sed -i 's/server_host=.*/server_host='"$MASTER_HOSTNAME"'/' /opt/cm-5.4.7/etc/cloudera-scm-agent/config.ini

# Create parcel directories
sudo mkdir -p /opt/cloudera/parcels

# Config the services to start automatically
sudo cp /opt/cm-5.4.7/etc/init.d/cloudera-scm-agent /etc/init.d/cloudera-scm-agent
sudo sed -i 's/CMF_DEFAULTS=.*/CMF_DEFAULTS=\/opt\/cm-5.4.7\/etc\/default/' /etc/init.d/cloudera-scm-agent
sudo chkconfig --add cloudera-scm-agent
sudo service cloudera-scm-agent start
echo "Cloudera installed successfully."

