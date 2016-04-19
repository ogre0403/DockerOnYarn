#!/bin/bash

# add multiple normal user with sudoers permission
# ./equip_sudo_user.sh <USER1> <PASSWORD1> <USER2> <PASSWORD2> ...


while [[ -n $1 ]]; do
    useradd -d /home/$1  -s /bin/bash -m -p $(echo $2 | openssl passwd -1 -stdin) $1
    grep -q  "^$1" /etc/sudoers || echo "$1    ALL=(ALL:ALL) ALL" >> /etc/sudoers;
    shift # shift all parameters;
    shift # shift all parameters;
done
