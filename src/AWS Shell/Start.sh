#!/bin/bash

/usr/local/hadoop/sbin/start-dfs.sh
/usr/local/hadoop/sbin/start-yarn.sh
sudo /usr/local/hadoop/sbin/mr-jobhistory-daemon.sh --config /usr/local/hadoop/etc/hadoop start historyserver
