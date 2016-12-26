#!/bin/bash


source "/vagrant/scripts/common.sh"

function installLocalHadoop {
	echo "install hadoop from local file"
	FILE=/vagrant/resources/$HADOOP_ARCHIVE
	tar -xzf $FILE -C /opt
}

function installRemoteHadoop {
	echo "install hadoop from remote file"
	curl -sS -o /vagrant/resources/$HADOOP_ARCHIVE -O -L $HADOOP_MIRROR_DOWNLOAD
	tar -xzf /vagrant/resources/$HADOOP_ARCHIVE -C /opt
}

function setupHadoop {
	echo "copying over hadoop configuration files"
	cp -f $HADOOP_RES_DIR/hdfs-site.xml $HADOOP_CONF
	cp -f $HADOOP_RES_DIR/core-site.xml $HADOOP_CONF
	cp -f $HADOOP_RES_DIR/yarn-site.xml $HADOOP_CONF
	cp -f $HADOOP_RES_DIR/mapred-site.xml $HADOOP_CONF
	cp -f $HADOOP_RES_DIR/slaves $HADOOP_CONF
	cp -f $HADOOP_RES_DIR/hadoop-env.sh $HADOOP_CONF
	cp -f $HADOOP_RES_DIR/hosts /etc
}

function setupEnvVars {
	echo "creating hadoop environment variables"
	cp -f $HADOOP_RES_DIR/hadoop.sh /etc/profile.d/hadoop.sh
	. /etc/profile.d/hadoop.sh
}

function installHadoop {
	if resourceExists $HADOOP_ARCHIVE; then
		installLocalHadoop
	else
		installRemoteHadoop
	fi
    chown -R hadoop:hadoop /opt/$HADOOP_VERSION
	ln -s /opt/$HADOOP_VERSION /opt/hadoop
}

function formatHdfs {
    echo "formatting HDFS"
    hdfs namenode -format
    chown -R hadoop:hadoop /home/hadoop/hadoop_dir
}

function createSSHKey {
    echo "generating ssh key"
    sudo -u hadoop ssh-keygen -t rsa -P "" -f /home/hadoop/.ssh/id_rsa
    sudo -u hadoop cat /home/hadoop/.ssh/id_rsa.pub >> /home/hadoop/.ssh/authorized_keys
    chmod 600 /home/hadoop/.ssh/authorized_keys
    chown hadoop:hadoop /home/hadoop/.ssh/authorized_keys
}


echo "setup hadoop"

#createSSHKey
installHadoop
setupHadoop
setupEnvVars
# formatHdfs

echo "hadoop setup complete"
