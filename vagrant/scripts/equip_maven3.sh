#!/bin/sh

#
# Ubuntu Equip 
# Maven 3 (latest version 3.2.5 http://maven.apache.org/download.cgi)
# Licence: MIT
# to run: wget --no-check-certificate https://github.com/resilva87/ubuntu-equip/raw/master/equip_maven3.sh && bash equip_maven3.sh [latest_version]

source "/vagrant/scripts/common.sh"

if [ -d "/opt/maven" ]; then
	echo "Maven already installed in /opt/maven, skipping!"
	exit 0
fi

LATEST="3.2.5"

if (( "$#" == 1 )); then
	LATEST=$1
fi

FILENAME="apache-maven-$LATEST-bin.tar.gz"
LINK="http://ftp.unicamp.br/pub/apache/maven/maven-3/$LATEST/binaries/$FILENAME"


mkdir maven
if resourceExists ${FILENAME}; then
	tar -zxvf /vagrant/resources/$FILENAME -C maven --strip-components 1
else
    wget --no-check-certificate "$LINK" -P /vagrant/resources
	tar -zxvf /vagrant/resources/$FILENAME -C maven --strip-components 1
fi

# Will copy to /opt
sudo mv maven /opt/

# set PATH env variable 
grep '/opt/maven/bin' /etc/profile || echo 'PATH=$PATH:/opt/maven/bin' >> /etc/profile


echo "Installed in /opt/maven"
