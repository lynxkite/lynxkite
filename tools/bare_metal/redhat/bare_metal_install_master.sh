set -ueo pipefail
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
sudo sed -i 's/server_host=.*/server_host=localhost/' /opt/cm-5.4.7/etc/cloudera-scm-agent/config.ini
sudo useradd --system --home=/opt/cm-5.4.7/run/cloudera-scm-server --no-create-home cloudera-scm
sudo chown -R cloudera-scm.cloudera-scm /opt/cm-5.4.7

# Create parcel directories
sudo mkdir -p /opt/cloudera/parcel-repo
sudo cp $DVD_ROOT/CDH-5.4.7-1.cdh5.4.7.p0.5-trusty.parcel /opt/cloudera/parcel-repo/CDH-5.4.7-1.cdh5.4.7.p0.5-trusty.parcel
sudo cp $DVD_ROOT/CDH-5.4.7-1.cdh5.4.7.p0.5-trusty.parcel.sha1 /opt/cloudera/parcel-repo/CDH-5.4.7-1.cdh5.4.7.p0.5-trusty.parcel.sha1
sudo cp $DVD_ROOT/manifest.json /opt/cloudera/parcel-repo/manifest.json
# Start a web server in the parcel repo to make Cloudera happy
cd /opt/cloudera/parcel-repo
python -m SimpleHTTPServer 8900 > /dev/null 2>&1 &
cd -

# Config the services to start automatically
sudo cp /opt/cm-5.4.7/etc/init.d/cloudera-scm-agent /etc/init.d/cloudera-scm-agent
sudo sed -i 's/CMF_DEFAULTS=.*/CMF_DEFAULTS=\/opt\/cm-5.4.7\/etc\/default/' /etc/init.d/cloudera-scm-agent
sudo chkconfig --add cloudera-scm-agent
sudo service cloudera-scm-agent start

# Configure PostgreSQL
echo "Configuring PostgreSQL..."
# Feed the following setup SQL script to psql
echo "create user clouderauser password 'clouderapassword';create database clouderadb;\q\n" | sudo -u postgres psql
sudo -u cloudera-scm /opt/cm-5.4.7/share/cmf/schema/scm_prepare_database.sh postgresql clouderadb clouderauser clouderapassword 
echo "PostgreSQL configured successfully."

sudo cp /opt/cm-5.4.7/etc/init.d/cloudera-scm-server /etc/init.d/cloudera-scm-server
sudo sed -i 's/CMF_DEFAULTS=.*/CMF_DEFAULTS=\/opt\/cm-5.4.7\/etc\/default/' /etc/init.d/cloudera-scm-server
sudo chkconfig --add cloudera-scm-server
sudo service cloudera-scm-server start
echo "Cloudera installed successfully."

