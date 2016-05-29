#!/bin/bash

nodeType=$(aws ec2 describe-tags --filters "Name=resource-id,Values=`/usr/bin/ec2metadata --instance-id`"  "Name=key,Values=Hadoop_Type" --output=text | cut  -f5)
nodeTag=$(aws ec2 describe-tags --filters "Name=resource-id,Values=`/usr/bin/ec2metadata --instance-id`"  "Name=key,Values=Hadoop_Node_Tag" --output=text | cut  -f5)
mylocalip=$(/usr/bin/ec2metadata --local-ip)
mymasterip=$(aws ec2 describe-tags --filters "Name=resource-id,Values=`/usr/bin/ec2metadata --instance-id`"  "Name=key,Values=Hadoop_Master_IP" --output=text | cut  -f5)
cassandra_cluster_size=$(aws ec2 describe-tags --filters "Name=resource-id,Values=`/usr/bin/ec2metadata --instance-id`"  "Name=key,Values=Cassandra_Cluster_Size" --output=text | cut  -f5)
cassandra_cluster_id=$(aws ec2 describe-tags --filters "Name=resource-id,Values=`/usr/bin/ec2metadata --instance-id`"  "Name=key,Values=Cassandra_Cluster_Id" --output=text | cut  -f5)

cd /usr/local/hadoop
echo $nodeType
echo $mylocalip
case "$nodeType" in

master)
#       if grep "master" /etc/hosts ; then
          sed -i 's/.*master//' /etc/hosts
#       fi

        echo $mylocalip "master" >> /etc/hosts





;;

slave)
        #masterIP=$(/usr/local/bin/aws ec2 describe-instances --filter "Name=tag:Hadoop_Type,Values=master" --output=json | grep -Po '"PrivateIpAddress":.*?[^\\]",' | cut -d ',' -f1 | sed 's/\"//g' | sed 's/ //g' | cut -d ":" -f2) 
        sed -i 's/.*master//' /etc/hosts
        echo $mymasterip "master" >> /etc/hosts
        ssh -l ubuntu master "sudo /home/ubuntu/addSlave $mylocalip $nodeTag"




;;

esac
