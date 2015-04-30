echo "Starting LynxKite installation..."

mkdir cloudera
sudo mount /dev/cdrom cloudera

# Install Java
echo "Installing Java..."
sudo mkdir -p /usr/java
sudo tar -xf cloudera/server-jre-7u80-linux-x64.tar.gz -C /usr/java
echo "Java installed successfully."

# Install Cloudera
echo "Installing Cloudera..."
sudo tar -xf cloudera/cloudera-manager-trusty-cm5.3.3_amd64.tar.gz -C /opt
echo "Cloudera installed successfully."

