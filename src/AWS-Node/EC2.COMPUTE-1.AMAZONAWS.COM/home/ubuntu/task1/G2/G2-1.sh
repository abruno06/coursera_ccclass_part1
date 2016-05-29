#!/bin/sh


HADOOP_CLASSPATH="/usr/lib/jvm/java-1.7.0-openjdk-amd64/lib/tools.jar" 
tmp=`echo /usr/share/cassandra/*.jar | sed 's/ /:/g'`
HADOOP_CLASSPATH="$HADOOP_CLASSPATH:$tmp"
tmp=`echo /usr/share/cassandra/lib/*.jar | sed 's/ /:/g'`
HADOOP_CLASSPATH="$HADOOP_CLASSPATH:$tmp"
#tmp=`echo /usr/share/java/*.jar | sed 's/ /:/g'`
#HADOOP_CLASSPATH="$HADOOP_CLASSPATH:$tmp"
#tmp=$(echo `/usr/local/hadoop/bin/hadoop classpath | sed 's/:/*.jar /g'` | sed 's/ /:/g')
#HADOOP_CLASSPATH="$HADOOP_CLASSPATH:$tmp"
export HADOOP_CLASSPATH
echo $HADOOP_CLASSPATH > classpath.txt
/usr/local/hadoop/bin/hadoop fs -rm -r -f /coursera/G2-Q1-output
rm build/*

/usr/local/hadoop/bin/hadoop com.sun.tools.javac.Main AirportCarrierDeparturePerformance.java -d ./build 
jar -cvf AirportCarrierDeparturePerformance.jar -C build/ .
#/usr/local/hadoop/bin/hadoop jar AirportCarrierDeparturePerformance.jar AirportCarrierDeparturePerformance -libjars `echo /usr/share/cassandra/*.jar | sed 's/ /,/g'` -libjars `echo /usr/share/cassandra/lib/*.jar | sed 's/ /,/g'` -libjars `echo /usr/share/java/*.jar | sed 's/ /,/g'` -libjars $(echo `/usr/local/hadoop/bin/hadoop classpath | sed 's/:/*.jar /g'` | sed 's/ /,/g') /coursera/input /coursera/G2-Q1-output
/usr/local/hadoop/bin/yarn jar AirportCarrierDeparturePerformance.jar AirportCarrierDeparturePerformance -libjars `echo /usr/share/cassandra/*.jar | sed 's/ /,/g'` -libjars `echo /usr/share/cassandra/lib/*.jar | sed 's/ /,/g'` /coursera/input_short /coursera/G2-Q1-output
/usr/local/hadoop/bin/hadoop fs -cat /coursera/G2-Q1-output/*


