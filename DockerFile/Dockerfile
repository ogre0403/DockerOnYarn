FROM ubuntu:latest
#ENV TERM linux
RUN apt-get update
#RUN apt-get dialog
RUN apt-get install -y sudo iputils-ping net-tools vim openssh-server
RUN mkdir -p /var/run/sshd
RUN mkdir -p /root/.ssh
RUN sed -ri 's/session    required     pam_loginuid.so/#session    required pam_loginuid.so/g' /etc/pam.d/sshd
ADD authorized_keys_peggytcpoc001 /root/.ssh/authorized_keys
ADD run.sh /run.sh
RUN chmod 755 /run.sh

EXPOSE 22
#CMD ["/run.sh"]
CMD env | grep _ >> /etc/environment ; ifconfig eth0 $HD_IP ; /run.sh 
#/usr/local/bin/run
