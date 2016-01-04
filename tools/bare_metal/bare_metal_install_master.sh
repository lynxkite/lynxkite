set -ueo pipefail
echo "Starting LynxKite installation..."

. config.sh
. common_config.sh

DVD_ROOT="$(dirname $0)"

# Install Java
echo "Installing Java..."
sudo mkdir -p /usr/java
sudo tar -xf $DVD_ROOT/server-jre-7u80-linux-x64.tar.gz -C /usr/java
sudo sh -c "echo export JAVA_HOME=/usr/java/jdk1.7.0_80/ > /etc/profile.d/lynx.sh"
echo "Java installed successfully."

# Install Cloudera
echo "Installing Cloudera..."
sudo tar -xf $DVD_ROOT/$CLOUDERA_MANAGER -C /opt
sudo sed -i 's/server_host=.*/server_host=localhost/' /opt/cm-$CLOUDERA_VERSION/etc/cloudera-scm-agent/config.ini
sudo useradd --system --home=/opt/cm-$CLOUDERA_VERSION/run/cloudera-scm-server --no-create-home cloudera-scm
sudo chown -R cloudera-scm.cloudera-scm /opt/cm-$CLOUDERA_VERSION

# Create parcel directories
sudo mkdir -p /opt/cloudera/parcel-repo
sudo cp $DVD_ROOT/$CLOUDERA_CDH_PARCEL /opt/cloudera/parcel-repo/$CLOUDERA_CDH_PARCEL
sudo cp $DVD_ROOT/$CLOUDERA_CDH_PARCEL_SHA1 /opt/cloudera/parcel-repo/$CLOUDERA_CDH_PARCEL_SHA1
sudo cp $DVD_ROOT/$CLOUDERA_MANIFEST /opt/cloudera/parcel-repo/$CLOUDERA_MANIFEST
# Start a web server in the parcel repo to make Cloudera happy
cd /opt/cloudera/parcel-repo
python -m SimpleHTTPServer 8900 > /dev/null 2>&1 &
cd -

# Config the services to start automatically
sudo cp /opt/cm-$CLOUDERA_VERSION/etc/init.d/cloudera-scm-agent /etc/init.d/cloudera-scm-agent
sudo sed -i 's/CMF_DEFAULTS=.*/CMF_DEFAULTS=\/opt\/cm-'$CLOUDERA_VERSION'\/etc\/default/' /etc/init.d/cloudera-scm-agent
AddService cloudera-scm-agent
sudo service cloudera-scm-agent start

# Configure PostgreSQL
echo "Configuring PostgreSQL..."
# Feed the following setup SQL script to psql
echo "create user clouderauser password 'clouderapassword';create database clouderadb;\q\n" | sudo -u postgres psql
sudo -u cloudera-scm /opt/cm-$CLOUDERA_VERSION/share/cmf/schema/scm_prepare_database.sh postgresql clouderadb clouderauser clouderapassword 
echo "PostgreSQL configured successfully."

sudo cp /opt/cm-$CLOUDERA_VERSION/etc/init.d/cloudera-scm-server /etc/init.d/cloudera-scm-server
sudo sed -i 's/CMF_DEFAULTS=.*/CMF_DEFAULTS=\/opt\/cm-'$CLOUDERA_VERSION'\/etc\/default/' /etc/init.d/cloudera-scm-server
AddService cloudera-scm-server
sudo service cloudera-scm-server start
echo "Cloudera installed successfully."

