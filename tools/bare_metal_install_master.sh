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
sudo useradd --system --home=/opt/cm-5.3.3/run/cloudera-scm-server --no-create-home cloudera-scm
sudo chown -R cloudera-scm.cloudera-scm /opt/cm-5.3.3
sudo cp /opt/cm-5.3.3/etc/init.d/cloudera-scm-agent /etc/init.d/cloudera-scm-agent
sudo cp /opt/cm-5.3.3/etc/init.d/cloudera-scm-server /etc/init.d/cloudera-scm-server
echo "Cloudera installed successfully."

# Configure PostgreSQL
echo "Configuring PostgreSQL..."
# Feed the following setup SQL script to psql
echo "create user clouderauser password 'clouderapassword';create database clouderadb;\q\n" | sudo -u postgres psql
sudo -u cloudera-scm /opt/cm5.3.3/share/cmf/schema/scm_prepare_database.sh postgresql clouderadb clouderauser clouderapassword 
echo "PostgreSQL configured successfully."

