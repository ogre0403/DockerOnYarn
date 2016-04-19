#!/bin/bash

# Equip Docker Ver. 1.7.1 for Ubuntu 14.04
# see
# https://docs.docker.com/engine/installation/linux/ubuntulinux/
# http://stackoverflow.com/questions/27657888/how-to-install-docker-specific-version



sudo apt-get update
sudo apt-get install apt-transport-https ca-certificates

sudo apt-key adv --keyserver hkp://p80.pool.sks-keyservers.net:80 --recv-keys 58118E89F3A912897C070ADBF76221572C52609D

echo 'deb https://apt.dockerproject.org/repo ubuntu-trusty main' | sudo tee --append /etc/apt/sources.list.d/docker.list

sudo apt-get update
sudo apt-get purge lxc-docker

sudo apt-get upgrade -y

# find available version
# sudo apt-cache policy docker-engine
sudo apt-get install docker-engine=1.7.1-0~trusty -y