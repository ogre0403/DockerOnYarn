
1. tcpoc002 tcpoc003要可連到tcpoc001的mysql  (privilege設定)

python 安裝mysqldb
apt-get install python-mysqldb

TCPOC001連自己
host="localhost"  ok,     host="172.17.1.1" 不行
user="root"
pwd="manager1"
db="ipManager"

TCPOC002連TCPOC001
>>
GRANT ALL ON *.* to root@'172.17.1.2' IDENTIFIED BY '1234';
GRANT ALL ON *.* to root@'172.17.1.3' IDENTIFIED BY '1234';
host="140.110.141.62"  OK    host="172.17.1.1" 不行
user="root"
pwd="1234"
db="ipManager"


	1. 將docker containerID, yarncontainerID, IP, node等資訊塞入mysql

create table dockerList(appID char(40),ip char(15), host char(50), dockerID char(12), user char(20), containerID char(40), starttime char(10), endtime char(13),status char(10)) ;

############
#  Command  #
############

START
TCPOC001:

	1.  透過YARN


peggy@TCPOC001:~/20151119DockerYarn$ java -cp `hadoop classpath`:yarnapp-1.0-SNAPSHOT.jar org.nchc.yarnapp.MyClient

	1. by command

python startDocker.py -p 172.17.1.25 -n TCPOC001 -y container123456789

STOP
TCPOC001:
python stopDocker.py -u peggy

