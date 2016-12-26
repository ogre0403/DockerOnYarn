#!/bin/bash

source "/vagrant/scripts/common.sh"

JDK_ARCHIVE=jdk-8u77-linux-x64.tar.gz
JDK_VERSION=jdk1.8.0_77

function installLocalJDK {
	echo "install jdk from local file"
	FILE=/vagrant/resources/${JDK_ARCHIVE}
	tar -xzf $FILE -C /opt
}

function installRemoteJDK {
	echo "install JDK from remote file"
	wget --no-check-certificate -c --header "Cookie: oraclelicense=accept-securebackup-cookie" http://download.oracle.com/otn-pub/java/jdk/8u77-b03/${JDK_ARCHIVE} -P /vagrant/resources
	tar -xzf /vagrant/resources/${JDK_ARCHIVE} -C /opt
}

function installJDK {
	if resourceExists $HADOOP_ARCHIVE; then
		installLocalJDK
	else
		installRemoteJDK
	fi

	ln -s /opt/${JDK_VERSION} /opt/java
}


installJDK