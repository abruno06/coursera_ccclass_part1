#!/bin/bash

filepath=/home/ubuntu/data2
fileyear=$1
filesrc=/mnt/data/aviation/airline_ontime
fileprefix=On_Time_On_Time_Performance_${fileyear}_


#unzip -o "${filesrc}/${fileyear}/*.zip" -d ${filepath}

#/home/ubuntu/extract "${filepath}/${fileprefix}1.csv" > ${filepath}/${fileprefix}1.extract.csv

cat "${filepath}/${fileprefix}1.csv" > "${filepath}/${fileprefix}full.csv"
for i in 2 3 4 5 6 7 8 9 10 11 12
do
echo ${filepath}/${fileprefix}${i}.csv
#/home/ubuntu/extract "${filepath}/${fileprefix}${i}.csv"  >> "${filepath}/${fileprefix}full.csv"
tail -n +2 "${filepath}/${fileprefix}${i}.csv" >> "${filepath}/${fileprefix}full.csv"
#rm "${filepath}/${fileprefix}${i}.csv"
#rm "${filepath}/${fileprefix}${i}.extract.csv"
#cat "${filepath}/${fileprefix}${i}.csv" >> "${filepath}/${fileprefix}full.csv"
done

/usr/local/hadoop/bin/hadoop fs -put "${filepath}/${fileprefix}full.csv" /coursera/input2

#rm -f ${filepath}/*
