#!/bin/bash

filepath=/tmp/data-2008/
fileyear=2008
filesrc=/mnt/data/aviation/airline_ontime
fileprefix=On_Time_On_Time_Performance_${fileyear}_


unzip -o "${filesrc}/${fileyear}/*.zip" -d ${filepath}

#cat "${filepath}/${fileprefix}1.csv" > "${filepath}/${fileprefix}full.csv"
/home/ubuntu/extract "${filepath}/${fileprefix}1.csv"  > "${filepath}/${fileprefix}full.csv"
for i in 2 3 4 5 6 7 8 9 10 11 12
do
echo ${filepath}/${fileprefix}${i}.csv
#tail -n +2 "${filepath}/${fileprefix}${i}.csv" >> "${filepath}/${fileprefix}full.csv"
/home/ubuntu/extract "${filepath}/${fileprefix}${i}.csv"  >> "${filepath}/${fileprefix}full.csv"
#cat "${filepath}/${fileprefix}${i}.csv" >> "${filepath}/${fileprefix}full.csv"
done

/usr/local/hadoop/bin/hadoop fs -put "${filepath}/${fileprefix}full.csv" /coursera/input2_short
# mvn exec:java -Dexec.mainClass="LoadFly" -Dexec.args="2500 /home/ubuntu/On_Time_On_Time_Performance_1988_1.csv"