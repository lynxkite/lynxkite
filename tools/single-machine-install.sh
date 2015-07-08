#!/bin/bash
## Script to install LynxKite in single machine configuration
## Requires the kite install...sh scripts in the same directory
## Requires sudo priviliges for java installation and lynxkite service setup
##
## Tested on Ubuntu 14, CentOs 6,7, Redhat 7, Amazon AMI 2015.03
## Should work on any recent Ubuntu/Debian, RedHat/Fedora/CentOs 
##     (Systemd support not implemented yet)  
##
## What the script does: 
##             -installs openjdk-7
##             -calls the kite install script
##             -downloads and unpacks the appropriate spark version
##             -creates .kiterc and kite-prefix-defitions.txt
##             -installs lynxkite as a system service and starts it 
##                  (does not work with systemd yet, where manual start/stop is required)
##
## Configuration
CUSTOM_PREFIX="LOCAL_DATA_IMPORT=\"file:${HOME}/local_data_import/\""
## This line will always be added to the prefix definitions
EMPTY_PREFIX=enabled
## EMPTY prefix enabled or disabled

#Process arguments

pushd $(dirname $0) > /dev/null
INSTALL_SCRIPTS_DIR=$(pwd -P)
popd > /dev/null


if [ ! -e ${INSTALL_SCRIPTS_DIR}/install-kite-$1.sh ]; then
    echo "Usage: single-machine-install.sh version [size]"
    echo "   where:"
    echo "	version can be a version number, 'stable', or 'testing'"
    echo "   and:"
    echo "	size specifies the memory and cpus available to LynxKite"
    echo "	and can be 'tiny', 'small', 'medium', 'large', or 'huge'"
    echo "	tiny (default): 1 CPU core, 256Mb RAM" 
    echo "	small: 1 CPU core, 1024Mb RAM"
    echo "	medium: 2 CPU cores, 3072Mb RAM"
    echo "	large: 3 CPU cores, 6144Mb RAM"
    echo "	huge: 6 CPU cores, 28000Mb RAM"
    echo "	The size configuration can be easily customized after"
    echo "	installation by editing the ~/.kiterc file"
    exit 1
fi

KITE_TOINSTALL=$1
KITE_SIZE=$(echo $2 | tr '[:upper:]' '[:lower:]')
if [ -z "${KITE_SIZE}" ]; then
    KITE_SIZE="tiny"
fi
echo "Installing LynxKite version ${KITE_TOINSTALL} in size ${KITE_SIZE}"    
KITE_NUM_CORES=1
KITE_MEMORY=256
if [ "${KITE_SIZE}" = "small" ]; then
    KITE_NUM_CORES=1
    KITE_MEMORY=1024
fi
if [ "${KITE_SIZE}" = "medium" ]; then
    KITE_NUM_CORES=2
    KITE_MEMORY=3072
fi
if [ "${KITE_SIZE}" = "large" ]; then
    KITE_NUM_CORES=3
    KITE_MEMORY=6144
fi
if [ "${KITE_SIZE}" = "huge" ]; then
    KITE_NUM_CORES=6
    KITE_MEMORY=28000
fi

#If called by vagrant, make the output nicer (disable wget progress bar)

if [ "$3" == "--vagrant" ]; then
    WGET_FLAG="-nv"
fi    

echo "Setting up Lynxkite for ${KITE_NUM_CORES} cores and ${KITE_MEMORY}MB of memory"

#Check sudo priviliges
if sudo -n true > /dev/null 2>%1; then
    echo "You have sudo priviliges without password"
else
    echo "Warning you might not have sudo priviliges"
    echo "Please enter your password when asked"
    echo "Without sudo priviliges, OpenJDK will not be installed,"
    echo "and you have to LynxKite manually"
fi

#Install wget and Openjdk 7
if type apt-get > /dev/null 2>%1; then
    echo "apt-get found. Installing wget and OpenJDK-7"
    sudo apt-get -y update
    sudo apt-get -y install openjdk-7-jre
    sudo apt-get -y install wget
elif type yum > /dev/null 2>%1; then
    echo "yum found. Installing wget and OpenJDK-7"
    sudo yum -y install java-1.7.0-openjdk.x86_64
    sudo yum -y install wget
elif type zypper > /dev/null 2>%1; then
    echo "zypper found. Installing wget and OpenJDK-7"
    sudo zypper --non-interactive install wget
    sudo zypper --non-interactive install java-1_7_0-openjdk
else
    echo "Package manager not found. Please install OpenJDK-7 manually"
fi

#Install Lynxkite

if [ -e "/etc/init/lynxkite.conf" ]; then
    echo "Shutting down lynxkite"
    sudo initctl stop lynxkite > /dev/null 2>&1
fi    
LYNXKITE_BASE_TEMP="${HOME}/lynxkite-temp"
rm -R ${LYNXKITE_BASE_TEMP} >/dev/null 2>&1
mkdir ${LYNXKITE_BASE_TEMP}
cd ${LYNXKITE_BASE_TEMP}
${INSTALL_SCRIPTS_DIR}/install-kite-${KITE_TOINSTALL}.sh
KITE_DIST_FOLDER_TEMP=$(ls -d ${LYNXKITE_BASE_TEMP}/kite*)
LYNXKITE_VERSION=$(cat ${KITE_DIST_FOLDER_TEMP}/version | grep release | awk -F'release ' '{print $2}' | awk -F' ' '{print $1}')
LYNXKITE_BASE="${HOME}/lynxkite-${LYNXKITE_VERSION}"
SPARK_VERSION=$(cat ${KITE_DIST_FOLDER_TEMP}/conf/SPARK_VERSION)
rm -R ${LYNXKITE_BASE}
mv ${LYNXKITE_BASE_TEMP} ${LYNXKITE_BASE}
KITE_DIST_FOLDER=$(ls -d ${LYNXKITE_BASE}/kite*)

#Install Spark

echo "Checking if spark-${SPARK_VERSION} is installed"
if [ ! -d "${HOME}/spark-${SPARK_VERSION}" ]; then
    echo "Downloading spark-${SPARK_VERSION}-bin-hadoop1.tgz"
    wget www.apache.org/dyn/closer.cgi/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop1.tgz -O ${HOME}/spark_mirrorlist.html
    SPARK_URL=$(cat ${HOME}/spark_mirrorlist.html | grep -m 1 "spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop1.tgz" | awk -F'<strong>' '{print $2}' | awk -F'</strong>' '{print $1}')
    rm ${HOME}/spark_mirrorlist.html
    echo "Downloading a large file. Be patient ..."
    wget ${SPARK_URL} -O ${HOME}/spark-${SPARK_VERSION}-bin-hadoop1.tgz ${WGET_FLAG}
    echo "Installing spark-${SPARK_VERSION}"
    tar xzf ${HOME}/spark-${SPARK_VERSION}-bin-hadoop1.tgz -C ${HOME}
    rm ${HOME}/spark-${SPARK_VERSION}-bin-hadoop1.tgz
    mv ${HOME}/spark-${SPARK_VERSION}-bin-hadoop1 ${HOME}/spark-${SPARK_VERSION}
else
    echo "Found spark-${SPARK_VERSION}, skipping spark installation"

fi

#Set up configuration files

#.kiterc

echo "Creating kiterc"
cat ${KITE_DIST_FOLDER}/conf/kiterc_template | \
     sed "/export SPARK_MASTER/c\export SPARK_MASTER=local" | \
     sed "/export KITE_META_DIR/c\export KITE_META_DIR=\$HOME/kite_meta" |  \
     sed "/export KITE_PREFIX_DEF/c\export KITE_PREFIX_DEFINITIONS=\$HOME/kite_prefix_definitions.txt" |  \
     sed "/export KITE_DATA_DIR/c\export KITE_DATA_DIR=file:\$HOME/kite_data" | \
     sed "/export NUM_CORES_PER_EXECUTOR/c\export NUM_CORES_PER_EXECUTOR=${KITE_NUM_CORES}"| \
     sed "/export KITE_MASTER_MEMORY/c\export KITE_MASTER_MEMORY_MB=${KITE_MEMORY}" > \
     ${HOME}/.kiterc

#prefix_definitions.txt

echo "Creating kite_prefix_defintions.txt"
echo ${CUSTOM_PREFIX} >  ${HOME}/kite_prefix_definitions.txt
if [ EMPTY_PREFIX = "enabled" ]; then 
    echo "EMPTY=\"\"" >>  ${HOME}/kite_prefix_definitions.txt
fi
CUSTOM_PREFIX_DIR=$(echo ${CUSTOM_PREFIX} | awk -F'file:' '{print $2}' | awk -F'"' '{print $1}')
echo "Creating directory ${CUSTOM_PREFIX_DIR}"
if [ ! -d ${CUSTOM_PREFIX_DIR} ]; then
    mkdir ${CUSTOM_PREFIX_DIR}
fi
    



# Install Lynxkite as a service and run kite (works with initctl but not yet with systemctl)
echo "Setting up LynxKite as server"

# Modifying /etc/sudoers to allow for sudo without tty so that
# Upstart can run Lynxkite as user and not root

sudo cp /etc/sudoers /etc/sudoers.orig
sudo cat /etc/sudoers.orig | sed "/requiretty/c\# " > ${HOME}/sudoers.new
sudo cp ${HOME}/sudoers.new /etc/sudoers
sudo rm ${HOME}/sudoers.new 


KITE_RUN_SCRIPT=$(ls  ${LYNXKITE_BASE}/run*)
if type initctl > /dev/null 2>%1; then
    echo "Creating upstart configuration file for initctl"
    cat <<EOM > ~/lynxkite.conf
#Lynxkite
description     "Lynxkite server"

start on runlevel [2345]
stop on runlevel [!2345]

respawn

exec sudo -H -u ${USER}  ${KITE_RUN_SCRIPT} interactive
EOM
    sudo mv ~/lynxkite.conf /etc/init/lynxkite.conf
    if sudo initctl start lynxkite; then
	echo "Lynxkite is running as a system service. "
    else
	echo "Cannot install LynxKite as a system service. "
	echo "Try starting manually to trubleshoot by running "
	echo "${KITE_RUN_SCRIPT} interactive"
    fi

#Check what's going on with systemctl

#elif type systemctl > /dev/null 2>%1; then
#    echo "Found systemctl. Creating systemd configuation file"    

else    
    if ${KITE_RUN_SCRIPT} start; then
	echo "Lynxkite is running, but it cannot be installed as a system service. "
    else
	echo "Cannot install LynxKite as a system service. "
    fi
	echo "Please start/stop LynxKite manually by running "
	echo "${KITE_RUN_SCRIPT} start/stop"
fi


