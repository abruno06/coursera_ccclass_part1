/*
* this is a module to resolve question 1 of group 1 in coursera
 */

 
import java.util.HashMap;
import java.util.HashSet;
import java.util.Arrays;
import java.util.Iterator;
import java.util.regex.Pattern;

import scala.Tuple2;

import kafka.serializer.StringDecoder;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.spark.streaming.Durations;


/**
 * Consumes messages from one or more topics in Kafka and does wordcount.
 *
 * Usage: TopAirport <broker> <topics> 
 *  
 *   <brokers> is a list of one or more Kafka brokers
 *   <topics> is a list of one or more kafka topics to consume from
 *
 * To run this example:
 *   `$ bin/run-example streaming.JavaDirectKafkaWordCount broker1-host:port,broker2-host:port topic1,topic2streaming.JavaDirectKafkaWordCount broker1-host:port,broker2-host:port topic1,topic2streaming.JavaDirectKafkaWordCount broker1-host:port,broker2-host:port topic1,topic2streaming.JavaDirectKafkaWordCount broker1-host:port,broker2-host:port topic1,topic2`
 */

public final class topairport {
  private static final Pattern SPACE = Pattern.compile(" ");

  private topairport() {
  }

  public static void main(String[] args) {
    if (args.length < 2) {
      System.err.println("Usage:  ./bin/spark-submit --packages org.apache.spark:spark-streaming-kafka_2.10:1.6.0 --class topairport <broker> <group> ");
      System.exit(1);
    }

    //StreamingExamples.setStreamingLogLevels();

    String brokers = args[0];
    String topics = args[1];
    
    SparkConf sparkConf = new SparkConf().setAppName("TopAirport");
    // Create the context with 2 seconds batch size
    //JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, new Duration(2000));
    JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(2));

    HashSet<String> topicsSet = new HashSet<String>(Arrays.asList(topics.split(",")));
    HashMap<String, String> kafkaParams = new HashMap<String, String>();
    kafkaParams.put("metadata.broker.list", brokers);
    
    // Create direct kafka stream with brokers and topicsvv
    JavaPairInputDStream<String, String> messages = KafkaUtils.createDirectStream(
            jssc,
            String.class,
            String.class,
            StringDecoder.class,
            StringDecoder.class,
            kafkaParams,
            topicsSet
        );


    // Get the lines, split them into words, count the words and print
    JavaDStream<String> lines = messages.map(new Function<Tuple2<String, String>, String>() {
   //   @Override
      public String call(Tuple2<String, String> tuple2) {
        return tuple2._2();
      }
    });
    JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
   //   @Override
      public Iterator<String> call(String x) {
        return Arrays.asList(SPACE.split(x)).iterator();
      }
    });
    JavaPairDStream<String, Integer> wordCounts = words.mapToPair(
      new PairFunction<String, String, Integer>() {
    //   @Override
        public Tuple2<String, Integer> call(String s) {
          return new Tuple2<String, Integer>(s, 1);
        }
      }).reduceByKey(
        new Function2<Integer, Integer, Integer>() {
    //   @Override
        public Integer call(Integer i1, Integer i2) {
          return i1 + i2;
        }
      });
    wordCounts.print();
    jssc.start();
    jssc.awaitTermination();
  }
}

