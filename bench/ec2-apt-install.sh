#!/bin/bash

set -e

sudo rm -rf /data
sudo mkdir /data
sudo chown -v -R ubuntu:ubuntu /data

sudo apt update --fix-missing

sudo apt install -y python python3 g++ gcc make
sudo apt install -y openjdk-8-jdk openjdk-8-jre
sudo apt install -y python-dev python3-dev uuid-dev zlib1g-dev libreadline-dev
sudo apt install -y python-pip python3-pip libpq-dev git
sudo apt install -y libpostgresql-jdbc-java

sudo pip3 install --upgrade setuptools
sudo pip3 install postgres


cd /data

git clone https://github.com/zheguang/encrypted-spark.git
git clone https://github.com/zheguang/Encrypted-SQL.git

(cd /data/Encrypted-SQL && bash build_postgres.sh)
