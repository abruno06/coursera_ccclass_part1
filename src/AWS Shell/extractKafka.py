#!/usr/bin/env python
#import threading, logging, time
import csv
import sys
csv.field_size_limit(1000000000)
from kafka.client import KafkaClient
from kafka.producer import SimpleProducer

client = KafkaClient("localhost:9092")
producer = SimpleProducer(client)

good_lines = 0
all_lines = 0
      
with open(sys.argv[1]) as csvfile:
	reader = csv.DictReader(csvfile)   	
	for row in reader:
 		all_lines+=1

        if row['Cancelled'] == "0.00" and row['Diverted'] == "0.00" and (row['FlightDate']== "2008-01-24" or row['FlightDate']== "2008-01-26"):
        	good_lines+=1
        	fly=",".join((row['DayOfWeek'],row['FlightDate'],row['UniqueCarrier'],row['FlightNum'],row['Origin'],row['Dest'],row['DepTime'],row['DepDelay'],row['DepDelayMinutes'],row['DepDel15'],row['ArrTime'],row['ArrDelay'],row['ArrDelayMinutes'],row['ArrDel15']))
        	#producer.send_messages('capstone', fly)
        	print (fly)
            #print "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s" % (row['FlightDate'],row['DayOfWeek'],row['TailNum'],row['FlightNum'],row['DepTime'],row['ArrTime'],row['UniqueCarrier'],row['Origin'],row['Dest'],row['DepDelay'],row['ArrDelay'],row['DepDelayMinutes'],row['ArrDelayMinutes'])
            #print row['FlightDate'],",",row['DayOfWeek'],",",row['TailNum'],",",row['FlightNum'],",",row['DepTime'],",",row['ArrTime'],",",row['UniqueCarrier'],",",row['Origin'],",",row['Dest'],",",row['DepDelay'],",",row['ArrDelay'],",",row['DepDelayMinutes'],",",row['ArrDelayMinutes']
            #output:
            #FlightDate,DayOfWeek,TailNum,FlightNum,DepTime,ArrTime,UniqueCarrier,Origin,Dest,DepDelay,ArrDelay,DepDelayMinutes,ArrDelayMinutes
			#     0          1       2      3         4        5        6           7     8     9         10          11           12
			#row['FlightDate'],row['DayOfWeek'],row['TailNum'],row['FlightNum'],row['DepTime'],row['ArrTime'],row['UniqueCarrier'],row['Origin'],row['Dest'],row['DepDelay'],row['ArrDelay'],row['DepDelayMinutes'],row['ArrDelayMinutes']
			#	DayOfWeek FlightDate	UniqueCarrier FlightNum	Origin DepTime	DepDelay	DepDelayMinutes	DepDel15  ArrTime	ArrDelay	ArrDelayMinutes	ArrDel15
			
sys.stderr.write("good lines %d for %d lines\n" % (good_lines,all_lines))	



    			
    