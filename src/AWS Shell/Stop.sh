#!/bin/bash

/usr/local/hadoop/sbin/mr-jobhistory-daemon.sh --config /usr/local/hadoop/etc/hadoop stop historyserver
/usr/local/hadoop/sbin/stop-yarn.sh
/usr/local/hadoop/sbin/stop-dfs.sh
sudo service cassandra stop
sudo rm -rf /var/lib/cassandra/*
