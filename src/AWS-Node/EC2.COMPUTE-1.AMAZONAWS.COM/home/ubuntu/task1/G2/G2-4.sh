#!/bin/sh


export HADOOP_CLASSPATH="/usr/lib/jvm/java-1.7.0-openjdk-amd64/lib/tools.jar" 
/usr/local/hadoop/bin/hadoop fs -rm -r -f /coursera/G2-Q4-output
rm build/*
/usr/local/hadoop/bin/hadoop com.sun.tools.javac.Main FlyDelayMean.java -d ./build 
jar -cvf FlyDelayMean.jar -C build/ .
#/usr/local/hadoop/bin/yarn jar FlyDelayMean.jar FlyDelayMean -libjars `echo /usr/share/cassandra/*.jar | sed 's/ /,/g'` -libjars `echo /usr/share/cassandra/lib/*.jar | sed 's/ /,/g'` -libjars `echo /usr/share/java/*.jar | sed 's/ /,/g'` -libjars $(echo `/usr/local/hadoop/bin/hadoop classpath | sed 's/:/*.jar /g'` | sed 's/ /,/g') /coursera/input_short /coursera/G2-Q4-output
/usr/local/hadoop/bin/yarn jar FlyDelayMean.jar FlyDelayMean  /coursera/input_short /coursera/G2-Q4-output
/usr/local/hadoop/bin/hadoop fs -cat /coursera/G2-Q4-output/*
