#!/bin/sh


export HADOOP_CLASSPATH=/usr/lib/jvm/java-1.7.0-openjdk-amd64/lib/tools.jar 
/usr/local/hadoop/bin/hadoop fs -rm -r -f /coursera/G3-Q2-output
rm build/*
/usr/local/hadoop/bin/hadoop com.sun.tools.javac.Main Travel.java -d ./build 
jar -cvf Travel.jar -C build/ .
/usr/local/hadoop/bin/yarn jar Travel.jar Travel /coursera/input2_short /coursera/G3-Q2-output
/usr/local/hadoop/bin/hadoop fs -cat /coursera/G3-Q2-output/*
