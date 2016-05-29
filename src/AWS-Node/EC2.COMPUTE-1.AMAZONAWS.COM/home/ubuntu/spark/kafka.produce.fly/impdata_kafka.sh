#!/bin/bash

filepath=/tmp/data/
fileyear=$1
filesrc=/mnt/data/aviation/airline_ontime
fileprefix=On_Time_On_Time_Performance_${fileyear}_


unzip -o "${filesrc}/${fileyear}/*.zip" -d ${filepath}



for i in 1 2 3 4 5 6 7 8 9 10 11 12
do
echo ${filepath}/${fileprefix}${i}.csv
mvn exec:java -Dexec.mainClass="LoadFly" -Dexec.args="${filepath}/${fileprefix}${i}.csv" 
#rm "${filepath}/${fileprefix}${i}.csv"
done
