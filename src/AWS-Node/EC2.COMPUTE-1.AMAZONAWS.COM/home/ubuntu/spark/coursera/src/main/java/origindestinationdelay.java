import java.util.HashMap;
import java.util.HashSet;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.regex.Pattern;
import java.util.List;
import java.util.Comparator;

import scala.Tuple2;

import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.*;
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
public final class origindestinationdelay {
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
		sparkConf.setAppName("origindestinationdelay");

		CassandraConnector connector = CassandraConnector.apply(sparkConf);
		//Session session;
		try (Session session = connector.openSession()) {
			session.execute("DROP KEYSPACE IF EXISTS origindestinationdelay");
			session.execute(
					"CREATE KEYSPACE origindestinationdelay WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3}");
			//session.execute(					"CREATE TABLE topcarrierinairport.table1( airport TEXT, carrier TEXT, departureperf float, PRIMARY KEY ((airport),departureperf,carrier) )");
			session.execute("CREATE TABLE origindestinationdelay.table1( origin TEXT, destination TEXT, meandelay float, PRIMARY KEY ((origin,destination)) )");
			// session.execute("CREATE TABLE java_api.sales (id UUID PRIMARY
			// KEY, product INT, price DECIMAL)");
			// session.execute("CREATE TABLE java_api.summaries (product INT
			// PRIMARY KEY, summary DECIMAL)");
		}

		final Function2<List<MeanDelay>, Optional<MeanDelay>, Optional<MeanDelay>> updateFunction = new Function2<List<MeanDelay>, Optional<MeanDelay>, Optional<MeanDelay>>() {
			// @Override
			public Optional<MeanDelay> call(List<MeanDelay> values, Optional<MeanDelay> state) {
				MeanDelay newSum = state.or(new MeanDelay(0,new Float(0.00)));
				for (MeanDelay value : values) {
					newSum.totalfly_ += value.totalfly_;
					newSum.totaldelay_ += value.totaldelay_;
				}
				return Optional.of(newSum);
			}
		};

		// Create context with 2 second batch interval
		// JavaStreamingContext jssc = new JavaStreamingContext(sparkConf,
		// Durations.seconds(2));
		JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(10));
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
				return Lists.newArrayList(fields[4] + ":" + fields[5] + "," + fields[12]);
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
		final JavaPairDStream<String, MeanDelay> delaycount = airport
				.mapToPair(new PairFunction<String, String, MeanDelay>() {
					// @Override
					public Tuple2<String, MeanDelay> call(String s) {
						String[] fields = s.split(",");
						
							return new Tuple2<String, MeanDelay>(fields[0], new MeanDelay(1, Float.valueOf(fields[1])));
						

					}
				}).reduceByKey(new Function2<MeanDelay, MeanDelay, MeanDelay>() {
					public MeanDelay call(MeanDelay t1, MeanDelay t2) {
						return new MeanDelay(t1.totalfly_ + t2.totalfly_, t1.totaldelay_ + t2.totaldelay_);
					}
				}).updateStateByKey(updateFunction);

		// delaycount.print();

		JavaPairDStream<Float, String> airportcarrierrank = delaycount
				.mapToPair(new PairFunction<Tuple2<String, MeanDelay>, Float, String>() {
					@Override
					public Tuple2<Float, String> call(Tuple2<String, MeanDelay> airportlist) {
						return new Tuple2<>(new Float(airportlist._2().avg()), airportlist._1());
					}
				});

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

		JavaDStream<AirportToAirportArrival> results = airportcarrierrank
				.map(new Function<Tuple2<Float, String>, AirportToAirportArrival>() {
					@Override
					public AirportToAirportArrival call(Tuple2<Float, String> aclist) {
						String[] key = aclist._2().split(":");
						return new AirportToAirportArrival(key[0], key[1], aclist._1());
					}

				});

		results.print();
		results.foreachRDD(new Function<JavaRDD<AirportToAirportArrival>, Void>() {
			@Override
			public Void call(JavaRDD<AirportToAirportArrival> rdd) {
			
				 javaFunctions(rdd).writerBuilder("origindestinationdelay","table1",mapToRow(AirportToAirportArrival.class) ).saveToCassandra();

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

	

	public static class AirportToAirportArrival implements Serializable {
		// ( airport TEXT, carrier TEXT, departureperf float, PRIMARY KEY
		// ((airport),departureperf) )");
		private String origin;
		private String destination;
		private Float meandelay;

		public AirportToAirportArrival(String origin, String destination, Float arrivaldelay) {
			this.origin = origin;
			this.destination = destination;
			this.meandelay = arrivaldelay;

		}

		public AirportToAirportArrival() {

		}

		public static AirportToAirportArrival newInstance(String origin, String destination, Float arrivaldelay) {
			AirportToAirportArrival airportcarrier = new AirportToAirportArrival();
			airportcarrier.setOrigin(origin);
			airportcarrier.setDestination(destination);
			airportcarrier.setMeandelay(arrivaldelay);
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

		public Float getMeandelay() {
			return meandelay;
		}

		public void setMeandelay(Float arrivaldelay) {
			this.meandelay = arrivaldelay;
		}

		@Override
		public String toString() {
			return Objects.toStringHelper(this).add("origin", origin).add("destination", destination)
					.add("meandelay", meandelay).toString();
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

	
	
	public static class MeanDelay implements Serializable {
		public MeanDelay(Integer totalfly, Float delay) {
			totalfly_ = totalfly;
			totaldelay_ = delay;
		}

		public Integer totalfly_;
		public Float totaldelay_;

		public float avg() {
			return (float) (totaldelay_ / (float) totalfly_);
		}

		public String toString() {
			return totaldelay_.toString() + "/" + totalfly_.toString();
		}
	}
}
