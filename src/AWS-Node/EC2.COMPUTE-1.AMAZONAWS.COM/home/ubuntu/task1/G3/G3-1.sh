#!/bin/sh


export HADOOP_CLASSPATH=/usr/lib/jvm/java-1.7.0-openjdk-amd64/lib/tools.jar 
/usr/local/hadoop/bin/hadoop fs -rm -r -f /coursera/G3-Q1-output
rm build/*
/usr/local/hadoop/bin/hadoop com.sun.tools.javac.Main PopularAirport.java -d ./build 
jar -cvf PopularAirport.jar -C build/ .
/usr/local/hadoop/bin/yarn jar PopularAirport.jar PopularAirport /coursera/input_short /coursera/G3-Q1-output
/usr/local/hadoop/bin/hadoop fs -cat /coursera/G3-Q1-output/*
