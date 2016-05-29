import java.util.HashMap;
import java.util.HashSet;
import java.util.Arrays;
import java.util.Collections;
import java.util.regex.Pattern;
import java.util.List;
import java.util.Comparator;
import scala.Tuple2;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.spark.streaming.Durations;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.StorageLevels;

import com.datastax.driver.core.Session;
import com.datastax.spark.connector.cql.CassandraConnector;
//import static com.datastax.spark.connector.japi.CassandraJavaUtil.*;

/**
 * Consumes messages from one or more topics in Kafka and does wordcount. Usage:
 * DirectKafkaWordCount <brokers> <topics> <brokers> is a list of one or more
 * Kafka brokers <topics> is a list of one or more kafka topics to consume from
 *
 * Example: $ bin/run-example streaming.KafkaWordCount
 * broker1-host:port,broker2-host:port topic1,topic2
 */
public final class topairport {
	private static final Pattern SPACE = Pattern.compile(" ");
	private static final Pattern COMMAS = Pattern.compile(",");

	public static void main(String[] args) {
		if (args.length < 2) {
			System.err.println(
					"Usage: topairport <brokers> <topics>\n" + "  <brokers> is a list of one or more Kafka brokers\n"
							+ "  <topics> is a list of one or more kafka topics to consume from\n\n");
			System.exit(1);
		}
		// StreamingExamples.setStreamingLogLevels();
		String brokers = args[0];
		String topics = args[1];
		// Create context with 2 second batch interval
		SparkConf sparkConf = new SparkConf();
		sparkConf.set("spark.cassandra.connection.host", "master");
		// sparkConf.setMaster("master:9092");
		sparkConf.setAppName("topairport");

		CassandraConnector connector = CassandraConnector.apply(sparkConf);

		try (Session session = connector.openSession()) {
			session.execute("DROP KEYSPACE IF EXISTS java_api");
			session.execute(
					"CREATE KEYSPACE java_api WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
			session.execute("CREATE TABLE java_api.products (id INT PRIMARY KEY, name TEXT, parents LIST<INT>)");
			session.execute("CREATE TABLE java_api.sales (id UUID PRIMARY KEY, product INT, price DECIMAL)");
			session.execute("CREATE TABLE java_api.summaries (product INT PRIMARY KEY, summary DECIMAL)");
		}

		final Function2<List<Integer>, Optional<Integer>, Optional<Integer>> updateFunction = new Function2<List<Integer>, Optional<Integer>, Optional<Integer>>() {
	//		@Override
			public Optional<Integer> call(List<Integer> values, Optional<Integer> state) {
				Integer newSum = state.or(0);
				for (Integer value : values) {
					newSum += value;
				}
				return Optional.of(newSum);
			}
		};

		JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(2));
		jssc.checkpoint("/tmp/log-analyzer-streaming");
		HashSet<String> topicsSet = new HashSet<String>(Arrays.asList(topics.split(",")));
		HashMap<String, String> kafkaParams = new HashMap<String, String>();
		kafkaParams.put("metadata.broker.list", brokers);
		// Create direct kafka stream with brokers and topics
		JavaPairInputDStream<String, String> messages = KafkaUtils.createDirectStream(jssc, String.class, String.class,
				StringDecoder.class, StringDecoder.class, kafkaParams, topicsSet);
		// Get the lines, split them into words, count the words and print
		JavaDStream<String> fly = messages.map(new Function<Tuple2<String, String>, String>() {
			// @Override
			public String call(Tuple2<String, String> tuple2) {
				return tuple2._2();
			}
		});
		JavaDStream<String> airport = fly.flatMap(new FlatMapFunction<String, String>() {
			// @Override
			public Iterable<String> call(String x) {
				String[] fields = x.split(",");
				return Lists.newArrayList(fields[4], fields[5]);
				// return Lists.newArrayList(COMMAS.split(x));
			}
		});
//		JavaPairDStream<String, Integer> airportcount = airport.mapToPair(new PairFunction<String, String, Integer>() {
//			// @Override
//			public Tuple2<String, Integer> call(String s) {
//				return new Tuple2<String, Integer>(s, 1);
//			}
//		}).reduceByKey(new Function2<Integer, Integer, Integer>() {
//			// @Override
//			public Integer call(Integer i1, Integer i2) {
//				return i1 + i2;
//			}
//		});
//		
		
		JavaPairDStream<String, Integer> airportcount = airport.mapToPair(new PairFunction<String, String, Integer>() {
			// @Override
			public Tuple2<String, Integer> call(String s) {
				return new Tuple2<String, Integer>(s, 1);
			}
		}).updateStateByKey(updateFunction);
		airportcount.print();
		JavaPairDStream<Integer, String> airportrank = airportcount.mapToPair(
				      new PairFunction<Tuple2<String, Integer>, Integer, String>() {
				        @Override
				        public Tuple2<Integer, String> call(Tuple2<String, Integer> airportlist) {
				          return new Tuple2<>(airportlist._2(),
				        		  airportlist._1());
				        }
				      });
		
		JavaPairDStream<Integer, String> topairport = 	airportrank.transformToPair(
			      new Function<JavaPairRDD<Integer, String>, JavaPairRDD<Integer, String>>() {
			        @Override
			        public JavaPairRDD<Integer, String> call(
			            JavaPairRDD<Integer, String> sortedairport) {
	
			        	
			        	sortedairport.sortByKey(false).saveAsTextFile("hdfs://master:54310/G1-Q1");
			        	
			        	return sortedairport.sortByKey(false);
			        }
			      }
			    );
	
		topairport.print(10);
//		topairport.foreachRDD(new VoidFunction<JavaPairRDD<Integer, String>>() {
//		      @Override
//		      public void call(JavaPairRDD<Integer, String> airportPairs) {
//		        List<Tuple2<Integer, String>> topList = airportPairs.take(10);
//		        System.out.println(
//		          String.format("\nlast run topairport (%s total):",
//		        		  airportPairs.count()));
//		        for (Tuple2<Integer, String> pair : topList) {
//		          System.out.println(
//		            String.format("%s (%s airport)", pair._2(), pair._1()));
//		        }
//		      }
//		    });
		
		// Start the computation
		jssc.start();
		jssc.awaitTermination();
	}
}