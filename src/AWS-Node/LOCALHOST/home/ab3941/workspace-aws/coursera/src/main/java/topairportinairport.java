import java.util.HashMap;
import java.util.HashSet;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.regex.Pattern;
import java.util.List;
import java.util.Comparator;

import scala.Tuple2;
import scala.reflect.ClassTag;

import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.*;
import org.apache.spark.rdd.RDD;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.spark.streaming.Durations;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.StorageLevels;

import com.datastax.driver.core.Session;
import com.datastax.spark.connector.cql.CassandraConnector;
import com.datastax.spark.connector.writer.RowWriterFactory;

//import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;
//import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapToRow;
import static com.datastax.spark.connector.japi.CassandraJavaUtil.*;
//import static com.datastax.spark.connector.CassandraJavaUtil.*;

/**
 * Consumes messages from one or more topics in Kafka and does wordcount. Usage:
 * DirectKafkaWordCount <brokers> <topics> <brokers> is a list of one or more
 * Kafka brokers <topics> is a list of one or more kafka topics to consume from
 *
 * Example: $ bin/run-example streaming.KafkaWordCount
 * broker1-host:port,broker2-host:port topic1,topic2
 */
public final class topairportinairport {
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
		SparkConf sparkConf = new SparkConf();
		sparkConf.set("spark.cassandra.connection.host", "master");
		// sparkConf.setMaster("master:9092");
		sparkConf.setAppName("topairportinairport");

		CassandraConnector connector = CassandraConnector.apply(sparkConf);
		//Session session;
		try (Session session = connector.openSession()) {
			session.execute("DROP KEYSPACE IF EXISTS topairportinairport");
			session.execute(
					"CREATE KEYSPACE topairportinairport WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3}");
			//session.execute(					"CREATE TABLE topcarrierinairport.table1( airport TEXT, carrier TEXT, departureperf float, PRIMARY KEY ((airport),departureperf,carrier) )");
			session.execute("CREATE TABLE topairportinairport.table1( origin TEXT, destination TEXT, departureperf float, PRIMARY KEY ((origin),destination) )");
			// session.execute("CREATE TABLE java_api.sales (id UUID PRIMARY
			// KEY, product INT, price DECIMAL)");
			// session.execute("CREATE TABLE java_api.summaries (product INT
			// PRIMARY KEY, summary DECIMAL)");
		}

		final Function2<List<Performance>, Optional<Performance>, Optional<Performance>> updateFunction = new Function2<List<Performance>, Optional<Performance>, Optional<Performance>>() {
			// @Override
			public Optional<Performance> call(List<Performance> values, Optional<Performance> state) {
				Performance newSum = state.or(new Performance(0, 0));
				for (Performance value : values) {
					newSum.totalfly_ += value.totalfly_;
					newSum.totalontime_ += value.totalontime_;
				}
				return Optional.of(newSum);
			}
		};

		// Create context with 2 second batch interval
		// JavaStreamingContext jssc = new JavaStreamingContext(sparkConf,
		// Durations.seconds(2));
		final JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(10));
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
				// Generate Origin:Carrier,delayindicator string
				// if (Boolean.valueOf(fields[10]))
				// {
				return Lists.newArrayList(fields[4] + ":" + fields[5] + "," + fields[9]);
				// }
				// else
				// {
				// return
				// Lists.newArrayList(fields[4]+":"+fields[2]+","+fields[9]+":"+fields[10]);
				// }
				//
				// return Lists.newArrayList(COMMAS.split(x));
			}
		});

		// JavaPairDStream<String, Integer> airportcount = airport.mapToPair(new
		// PairFunction<String, String, Integer>() {
		// // @Override
		// public Tuple2<String, Integer> call(String s) {
		// return new Tuple2<String, Integer>(s, 1);
		// }
		// }).reduceByKey(new Function2<Integer, Integer, Integer>() {
		// // @Override
		// public Integer call(Integer i1, Integer i2) {
		// return i1 + i2;
		// }
		// });
		//
		// Function<String,Performance> createPerformance = new
		// Function<String,Performance>(){
		// public Performance call (String s){
		// String[] fields = s.split(":");
		// return new Performance(1,fields[1],Float.valueOf(fields[0]));
		// }
		// };

		// to calculate performance will consider a fly below 15min delay vs
		// above this value.
		final JavaPairDStream<String, Performance> delaycount = airport
				.mapToPair(new PairFunction<String, String, Performance>() {
					// @Override
					public Tuple2<String, Performance> call(String s) {
						String[] fields = s.split(",");
						if (fields[1].equals(new String("0.00"))) {
							return new Tuple2<String, Performance>(fields[0], new Performance(1, 1));
						} else {
							return new Tuple2<String, Performance>(fields[0], new Performance(1, 0));
						}

					}
				}).reduceByKey(new Function2<Performance, Performance, Performance>() {
					public Performance call(Performance t1, Performance t2) {
						return new Performance(t1.totalfly_ + t2.totalfly_, t1.totalontime_ + t2.totalontime_);
					}
				}).updateStateByKey(updateFunction);

		// delaycount.print();

		
		
		JavaPairDStream<Float, String> airportcarrierrank = delaycount
				.mapToPair(new PairFunction<Tuple2<String, Performance>, Float, String>() {
					@Override
					public Tuple2<Float, String> call(Tuple2<String, Performance> airportlist) {
						return new Tuple2<>(new Float(airportlist._2().avg()), airportlist._1());
					}
				});

//		JavaPairDStream<Float, String> topairportcarrier = airportcarrierrank.transformToPair(new Function<JavaPairRDD<Float,String>,JavaPairRDD<Float,String>>(){
//			@Override
//			public JavaPairRDD<Float, String> call(JavaPairRDD<Float,String> rdd) {
//				
//				return jssc.sparkContext().parallelizePairs(rdd.sortByKey(false).top(10));
//			//	for (Tuple2<Float,String> value : rdd.sortByKey(false).top(10)) {
//				//	return new JavaPairRDD<Float, String>(value._1(),value._2());
//				//	new JavaPairRDD<Float, String>(, null, null)//(null, ClassTag(Float), null).toRDD(value._1(),value._2())
//			//		return new JavaPairRDD<Float,String>(new RDD<Tuple2<Float, String>>(new Tuple2<Float, String>(value._1(),value._2())), null, null);
//			//	}
//			}
//		});
//		
//		topairportcarrier.print();
		
//		JavaPairDStream<Float, String> topairportcarrier = airportcarrierrank
//				.transformToPair(new Function<JavaPairRDD<Float, String>, JavaPairRDD<Float, String>>() {
//					@Override
//					public JavaPairRDD<Float, String> call(JavaPairRDD<Float, String> sortedairport) {
//
//						// sortedairport.sortByKey(false).saveAsTextFile("hdfs://master:54310/G1-Q1");
//
//						return sortedairport.sortByKey(false);
//						
//						// CassandraJavaUtil.javaFunctions(sc.makeRDD(Arrays.asList(tuple)))
//						// .writerBuilder("cassandra_java_util_spec",
//						// "test_table_4", mapTupleToRow(
//						// String.class,
//						// Integer.class,
//						// Double.class
//						// )).withColumnSelector(someColumns("stringCol",
//						// "intCol", "doubleCol"))
//						// .saveToCassandra();
//					}
//				});
		//	topairportcarrier.print();

		// JavaDStream<AirportCarrier> results =
		// airportcarrierrank.transform(new Function<JavaPairRDD<Float,
		// String>,JavaRDD<AirportCarrier>>(){
		// @Override
		// public JavaRDD<AirportCarrier> call(
		// JavaPairRDD<Float, String> airportcarrier) {
		// String[] key = airportcarrier.first()._2.split(":");
		// AirportCarrier ACobj = new
		// AirportCarrier(key[0],key[1],airportcarrier.first()._1);
		// return ACobj;
		//
		// }
		// }
		// );

		JavaDStream<AirportToAirport> results = airportcarrierrank
				.map(new Function<Tuple2<Float, String>, AirportToAirport>() {
					@Override
					public AirportToAirport call(Tuple2<Float, String> aclist) {
						String[] key = aclist._2().split(":");
						return new AirportToAirport(key[0], key[1], aclist._1());
					}

				});

		results.print();
		results.foreachRDD(new Function<JavaRDD<AirportToAirport>, Void>() {
			@Override
			public Void call(JavaRDD<AirportToAirport> rdd) {
			
				 javaFunctions(rdd).writerBuilder("topairportinairport","table1",mapToRow(AirportToAirport.class) ).saveToCassandra();

				// javaFunctions(rdd)).saveToCassandra("topcarrierinairport",
				// "table1",mapToRow(AirportCarrier.class));
				return null;
			}
		});
		//
		//// List<Person> people = Arrays.asList(
		//// new Person(1, "John", new Date()),
		//// new Person(2, "Troy", new Date()),
		//// new Person(3, "Andrew", new Date())
		//// );
		//// JavaRDD<AirportCarrier> rdd = sc.parallelize(people);
		// javaFunctions(rdd).writerBuilder("topcarrierinairport", "table1",
		// mapToRow(AirportCarrier.class)).saveToCassandra();
		////
		////
		// topairport.print(10);
		//// topairport.foreachRDD(new VoidFunction<JavaPairRDD<Integer,
		// String>>() {
		//// @Override
		//// public void call(JavaPairRDD<Integer, String> airportPairs) {
		//// List<Tuple2<Integer, String>> topList = airportPairs.take(10);
		//// System.out.println(
		//// String.format("\nlast run topairport (%s total):",
		//// airportPairs.count()));
		//// for (Tuple2<Integer, String> pair : topList) {
		//// System.out.println(
		//// String.format("%s (%s airport)", pair._2(), pair._1()));
		//// }
		//// }
		//// });
		//
		// Start the computation
		jssc.start();
		jssc.awaitTermination();
	}

	

	public static class AirportToAirport implements Serializable {
		// ( airport TEXT, carrier TEXT, departureperf float, PRIMARY KEY
		// ((airport),departureperf) )");
		private String origin;
		private String destination;
		private Float departureperf;

		public AirportToAirport(String origin, String destination, Float departureperf) {
			this.origin = origin;
			this.destination = destination;
			this.departureperf = departureperf;

		}

		public AirportToAirport() {

		}

		public static AirportToAirport newInstance(String origin, String destination, Float departureperf) {
			AirportToAirport airportcarrier = new AirportToAirport();
			airportcarrier.setOrigin(origin);
			airportcarrier.setDestination(destination);
			airportcarrier.setDepartureperf(departureperf);
			return airportcarrier;
		}

		public String getOrigin() {
			return origin;
		}

		public void setOrigin(String airport) {
			this.origin = airport;
		}

		public String getDestination() {
			return destination;
		}

		public void setDestination(String airport) {
			this.destination = airport;
		}

		public Float getDepartureperf() {
			return departureperf;
		}

		public void setDepartureperf(Float departureperf) {
			this.departureperf = departureperf;
		}

		@Override
		public String toString() {
			return Objects.toStringHelper(this).add("origin", origin).add("destination", destination)
					.add("departureperf", departureperf).toString();
		}

	}

	public static class Performance implements Serializable {
		public Performance(Integer totalfly, Integer totalontime) {
			totalfly_ = totalfly;
			totalontime_ = totalontime;
		}

		public Integer totalfly_;
		public Integer totalontime_;

		public float avg() {
			return (float) ((totalontime_ / (float) totalfly_) * 100.00);
		}

		public String toString() {
			return totalontime_.toString() + "/" + totalfly_.toString();
		}
	}

}
