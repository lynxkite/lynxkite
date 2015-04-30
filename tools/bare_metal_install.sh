echo "Please insert the LynxKite install DVD then press any key."
read

mkdir cloudera
sudo mount /dev/cdrom cloudera

# Install Java
echo "Installing Java..."
sudo mkdir -p /usr/java
sudo tar -xf cloudera/server-jre-7u80-linux-x64.tar.gz -C /usr/java
echo "Java isntalled successfully."

# Install Cloudera
echo "Installing Cloudera..."
sudo tar -xf cloudera/cloudera-manager-trusty-cm5.3.3_amd64.tar.gz -C /opt
sudo useradd --system --home=/opt/cm-5.3.3/run/cloudera-scm-server --no-create-home cloudera-scm
sudo chown -R cloudera-scm.cloudera-scm /opt/cm-5.3.3
echo "Cloudera isntalled successfully."

# Configure PostgreSQL
echo "Configuring PostgreSQL..."
# Feed the following setp SQL script to psql
echo "create user clouderauser password 'clouderapassword';create database clouderadb;\q\n" | sudo -u postgres psql
sudo -u cloudera-scm /opt/cm5.3.3/share/cmf/schema/scm_prepare_database.sh postgresql clouderadb clouderauser clouderapassword 
echo "PostgreSQL configured successfully."

