#!/bin/bash
## Script to install LynxKite in single machine configuration
## Requires the kite install...sh scripts in the same directory
## Should be run as root
## 
## Tested on Ubuntu 14, CentOs 6,7, Redhat 7, Amazon AMI 2015.03, SUSE 12
## Should work on any recent Ubuntu/Debian, RedHat/Fedora/CentOs, SUSE
##
## What the script does: 
##             -installs wget and openjdk-7
##             -calls the kite install script
##             -downloads and unpacks the appropriate spark version
##             -creates .kiterc and kite-prefix-defitions.txt
##             -installs lynxkite as a system service and starts it
##
## Configuration
HADOOP_VERSIONS="2.6 2.4 2.3 2 1"
## When downloading Spark check these hadoop versions in this order



#Preparations
print_usage(){
    echo "Usage: single-machine-install.sh user version [size]"
    echo ""
    echo -e "	\e[1;04musername\e[0m is the target user for the installation."
    echo -e "	Kite will be installed under this user's home directory."
    echo -e "	The user account will be created if it does not exist."
    echo -e ""
    echo -e "	\e[1;04mversion\e[0m can be a version number, 'stable', or 'testing'"
    echo -e ""
    echo -e "	\e[1;04msize\e[0m specifies the memory and cpus available to LynxKite"
    echo -e "	and can be 'tiny', 'small', 'medium', 'large', or 'huge'"
    echo -e "		tiny (default): 1 CPU core, 256Mb RAM" 
    echo -e "		small: 1 CPU core, 1024Mb RAM"
    echo -e "		medium: 2 CPU cores, 3072Mb RAM"
    echo -e "		large: 3 CPU cores, 6144Mb RAM"
    echo -e "		huge: 6 CPU cores, 28000Mb RAM"
    echo -e ""
    echo -e "	The size configuration can be easily customized after"
    echo -e "	installation by editing the ~/.kiterc file"
    echo -e ""
    exit 1
}


set -euo pipefail
pushd $(dirname $0) > /dev/null
INSTALL_SCRIPTS_DIR=$(pwd -P)
popd > /dev/null


#Check for root privileges
if [ "$(id -u)" != "0" ]; then
    echo "You must be root to run this script"
    exit 1
fi

#Process arguments
if [ $# -lt 2 ]; then
    print_usage
fi

TARGET_USER=$1
KITE_TOINSTALL=$2
if [ ! -e ${INSTALL_SCRIPTS_DIR}/install-kite-${KITE_TOINSTALL}.sh ]; then
    echo "Error: Cannot find ${INSTALL_SCRIPTS_DIR}/install-kite-${KITE_TOINSTALL}.sh"
    echo "Make sure you have this install script in ${INSTALL_SCRIPTS_DIR}"
    exit 1
fi

KITE_SIZE="tiny"
if [ $# -ge 2 ]; then
    KITE_SIZE=$3
fi    
KITE_NUM_CORES=1
KITE_MEMORY=256
if [ "${KITE_SIZE}" = "small" ]; then
    KITE_NUM_CORES=1
    KITE_MEMORY=1024
elif [ "${KITE_SIZE}" = "medium" ]; then
    KITE_NUM_CORES=2
    KITE_MEMORY=3072
elif [ "${KITE_SIZE}" = "large" ]; then
    KITE_NUM_CORES=3
    KITE_MEMORY=6144
elif [ "${KITE_SIZE}" = "huge" ]; then
    KITE_NUM_CORES=6
    KITE_MEMORY=28000
else
    KITE_SIZE="tiny"
    KITE_NUM_CORES=1
    KITE_MEMORY=256
fi




echo "Installing LynxKite version ${KITE_TOINSTALL} in size ${KITE_SIZE}"    
echo "Setting up LynxKite for ${KITE_NUM_CORES} core(s) and ${KITE_MEMORY}MBytes of memory"

#Check if target user and group exists and create if not
if id -u ${TARGET_USER} > /dev/null 2>&1; then
    echo "Installing for user ${TARGET_USER}"
else
    echo "User ${TARGET_USER} does not exist. Creating user account."
    useradd ${TARGET_USER}
fi
TARGET_GROUP=$(su ${TARGET_USER} -c 'id -gn' )

if id -g ${TARGET_USER} > /dev/null 2>&1; then
    echo "Group ${TARGET_USER} exists"
else
    echo "Group ${TARGET_USER} does not exist. Creating group."
    groupadd ${TARGET_USER}
    usermod
fi
TARGET_HOME=$(eval echo ~$TARGET_USER)

if [ ! -d "${TARGET_HOME}" ]; then
    mkdir ${TARGET_HOME}
    chown ${TARGET_USER} ${TARGET_HOME}
    chgrp ${TARGET_GROUP} ${TARGET_HOME}
fi


#Install wget and Openjdk 7
if type apt-get > /dev/null 2>&1; then
    echo "apt-get found. Installing wget and OpenJDK-7"
    apt-get -y update
    apt-get -y install openjdk-7-jre
    apt-get -y install wget
elif type yum > /dev/null 2>&1; then
    echo "yum found. Installing wget and OpenJDK-7"
    yum -y install java-1.7.0-openjdk.x86_64
    yum -y install wget
elif type zypper > /dev/null 2>&1; then
    echo "zypper found. Installing wget and OpenJDK-7"
    zypper --non-interactive install wget
    zypper --non-interactive install java-1_7_0-openjdk
else
    echo "Package manager not found. Please install OpenJDK-7 manually"
fi

#Shutdown lynxkite if running
if [ -e "/etc/init/lynxkite.conf" ]; then
    if ( initctl status lynxkite | grep start ); then
        echo "Shutting down LynxKite using initctl"
    	initctl stop lynxkite
    fi
elif [ -e "/usr/lib/systemd/system/lynxkite.service" ]; then
    if ( systemctl status lynxkite | grep active ); then
	echo "Shutting down LynxKite using systemctl"
	systemctl stop lynxkite
    fi
fi    

#Install LynxKite
LYNXKITE_BASE_TEMP="${TARGET_HOME}/lynxkite-temp"
if [ -e ${LYNXKITE_BASE_TEMP} ]; then 
    rm -R ${LYNXKITE_BASE_TEMP} >/dev/null 2>&1
fi
mkdir ${LYNXKITE_BASE_TEMP}
cd ${LYNXKITE_BASE_TEMP}
echo "Running lynxkite installer"
${INSTALL_SCRIPTS_DIR}/install-kite-${KITE_TOINSTALL}.sh
KITE_DIST_FOLDER_TEMP=$(ls -d ${LYNXKITE_BASE_TEMP}/kite*)
LYNXKITE_VERSION="${KITE_TOINSTALL}"
LYNXKITE_BASE="${TARGET_HOME}/lynxkite-${LYNXKITE_VERSION}"
SPARK_VERSION=$(cat ${KITE_DIST_FOLDER_TEMP}/conf/SPARK_VERSION)
if [ -e  ${LYNXKITE_BASE} ]; then
    rm -R ${LYNXKITE_BASE}
fi
mv ${LYNXKITE_BASE_TEMP} ${LYNXKITE_BASE}
chown -R ${TARGET_USER} ${LYNXKITE_BASE}
chgrp -R ${TARGET_GROUP} ${LYNXKITE_BASE}
KITE_DIST_FOLDER=$(ls -d ${LYNXKITE_BASE}/kite*)

#Install Spark

echo "Checking if spark-${SPARK_VERSION} is installed"
if [ ! -e "${TARGET_HOME}/spark-${SPARK_VERSION}" ]; then
    for version in ${HADOOP_VERSIONS}; do
	SPARK_FILE=spark-${SPARK_VERSION}-bin-hadoop${version}
	SPARK_URL="http://d3kbcqa49mib13.cloudfront.net/${SPARK_FILE}.tgz"
        if wget ${SPARK_URL} -O ${TARGET_HOME}/${SPARK_FILE}.tgz; then
		break
	fi
	rm ${TARGET_HOME}/${SPARK_FILE}.tgz    
    done
    echo "Installing spark-${SPARK_VERSION}"
    tar xzf ${TARGET_HOME}/${SPARK_FILE}.tgz -C ${TARGET_HOME}
    rm ${TARGET_HOME}/${SPARK_FILE}.tgz
    chown -R ${TARGET_USER} ${TARGET_HOME}/${SPARK_FILE} 
    chgrp -R ${TARGET_GROUP} ${TARGET_HOME}/${SPARK_FILE}
    ln -s ${TARGET_HOME}/${SPARK_FILE} ${TARGET_HOME}/spark-${SPARK_VERSION}
    
else
    echo "Found spark-${SPARK_VERSION}, skipping spark installation"
fi

#Set up configuration files

#.kiterc

echo "Creating kiterc"
cat ${KITE_DIST_FOLDER}/conf/kiterc_template | \
     sed "/export KITE_PREFIX_DEF/c\export KITE_PREFIX_DEFINITIONS=\$HOME/kite_prefix_definitions.txt" |  \
     sed "/export NUM_CORES_PER_EXECUTOR/c\export NUM_CORES_PER_EXECUTOR=${KITE_NUM_CORES}"| \
     sed "/export KITE_MASTER_MEMORY/c\export KITE_MASTER_MEMORY_MB=${KITE_MEMORY}" > \
     ${TARGET_HOME}/.kiterc
chown -R ${TARGET_USER} ${TARGET_HOME}/.kiterc 
chgrp -R ${TARGET_GROUP} ${TARGET_HOME}/.kiterc

#kite-prefix-defitions

if [ -e "${KITE_DIST_FOLDER}/conf/prefix_definitions_template.txt" ]; then
    cp ${KITE_DIST_FOLDER}/conf/prefix_definitions_template.txt ${TARGET_HOME}/kite_prefix_definitions.txt
else
    echo "" > ${TARGET_HOME}/kite_prefix_definitions.txt
fi
chown -R ${TARGET_USER} ${TARGET_HOME}/kite_prefix_definitions.txt 
chgrp -R ${TARGET_GROUP} ${TARGET_HOME}/kite_prefix_definitions.txt

# Test if kite starts

KITE_RUN_SCRIPT=$(ls ${LYNXKITE_BASE}/run*)

echo "Testing LynxKite startup"
if su ${TARGET_USER} ${KITE_RUN_SCRIPT} start; then
    echo "Startup test successful, shutting down..."
    sleep 5
    su ${TARGET_USER} ${KITE_RUN_SCRIPT} stop > /dev/null 2>&1
fi

# Install LynxKite as a service and run kite 
echo "Setting up LynxKite as server"

#Create /etc/init/lynxkite.conf

if type initctl > /dev/null 2>&1; then
    echo "Creating upstart configuration file for initctl"
    cat <<EOM > /etc/init/lynxkite.conf
#LynxKite
description "LynxKite server"

start on runlevel [2345]
stop on runlevel [!2345]

respawn
pre-start script
    rm -f ${TARGET_HOME}/kite.pid
end script
exec su ${TARGET_USER} ${KITE_RUN_SCRIPT} interactive
post-stop script
    rm -f ${TARGET_HOME}/kite.pid
end script
EOM
    if  initctl start lynxkite; then
	echo "Installation complete."
	echo "LynxKite is running as a system service."
    else
	echo "Cannot install LynxKite as a system service."
	echo "Try starting manually to troubleshoot by running:"
	echo "${KITE_RUN_SCRIPT} interactive"
    fi

#Create /usr/lib/systemd/system/lynxkite.service

elif type systemctl > /dev/null 2>&1; then
################
# Temporary patch for cat /dev/urandom  
# We can remove this later, but wanted to have a working script with the older releases
    cp ${KITE_DIST_FOLDER}/bin/biggraph ${KITE_DIST_FOLDER}/bin/biggraph.beforepatch
    cat ${KITE_DIST_FOLDER}/bin/biggraph.beforepatch | \
     sed "/export KITE_RANDOM_SECRET=.cat \/dev\/urandom/c\export KITE_RANDOM_SECRET=\$(python -c 'import random, string; print(\"\".join(random.choice(string.ascii_letters) for i in range(32)))')" > \
     ${KITE_DIST_FOLDER}/bin/biggraph
    chown -R ${TARGET_USER} ${KITE_DIST_FOLDER}/bin/biggraph 
    chgrp -R ${TARGET_GROUP} ${KITE_DIST_FOLDER}/bin/biggraph
################
    echo "Creating systemd configuation file"    
    cat <<EOM > /usr/lib/systemd/system/lynxkite.service
[Unit]
Description=LynxKite Server

[Service]
ExecStartPre=-rm -f ${TARGET_HOME}/kite.pid
ExecStart=${KITE_RUN_SCRIPT} interactive
ExecStopPost=-rm -f ${TARGET_HOME}/kite.pid
WorkingDirectory=${LYNXKITE_BASE}
User=${TARGET_USER}
Restart=always

[Install]
WantedBy=multi-user.target
EOM
    systemctl daemon-reload
    systemctl enable lynxkite >/dev/null
    if  systemctl start lynxkite; then
	echo "Installation complete."
	echo "LynxKite is running as a system service."
    else
	echo "Cannot install LynxKite as a system service."
	echo "Try starting manually to troubleshoot by running:"
	echo "${KITE_RUN_SCRIPT} interactive"
    fi
else    
    if su ${TARGET_USER} ${KITE_RUN_SCRIPT} start; then
	echo "LynxKite is running, but it cannot be installed as a system service."
    else
	echo "Cannot install LynxKite as a system service."
    fi
    echo "Please start/stop LynxKite manually by running"
    echo "${KITE_RUN_SCRIPT} start/stop"
fi
