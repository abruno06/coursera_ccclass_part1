#!/bin/bash

filepath=/tmp/data/
fileyear=$1
filesrc=/mnt/data/aviation/airline_ontime
fileprefix=On_Time_On_Time_Performance_${fileyear}_


unzip -o "${filesrc}/${fileyear}/*.zip" -d ${filepath}

for i in 1 2 3 4 5 6 7 8 9 10 11 12
do
echo ${filepath}/${fileprefix}${i}.csv
#mvn exec:java -Dexec.mainClass="LoadFly" -Dexec.args="22 ${filepath}/${fileprefix}${i}.csv"
~/extract2 "${filepath}/${fileprefix}${i}.csv" | pv -L 15m  | /home/kafka/kafka/bin/kafka-console-producer.sh --broker-list=localhost:9092 --topic=capstone10
rm "${filepath}/${fileprefix}${i}.csv" 
done
