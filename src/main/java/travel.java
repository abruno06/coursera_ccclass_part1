import java.util.HashMap;
import java.util.HashSet;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Calendar;
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
public final class travel {
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
		sparkConf.setAppName("travel");
		CassandraConnector connector = CassandraConnector.apply(sparkConf);
		// Session session;
		try (Session session = connector.openSession()) {
		//	session.execute("DROP KEYSPACE IF EXISTS travel");
		//	session.execute(
		//			"CREATE KEYSPACE travel WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3}");
			// session.execute( "CREATE TABLE topcarrierinairport.table1(
			// airport TEXT, carrier TEXT, departureperf float, PRIMARY KEY
			// ((airport),departureperf,carrier) )");
		//	session.execute(
		//			"CREATE TABLE travel.table1( origin TEXT, destination TEXT, stopby TEXT, date TEXT, xydeptime TEXT, xyflight TEXT, xydelay FLOAT, yzdate TEXT, yzdeptime TEXT, yzflight TEXT, yzdelay FLOAT , PRIMARY KEY ((origin,destination,stopby),date))");

			// );
			// session.execute("CREATE TABLE java_api.sales (id UUID PRIMARY
			// KEY, product INT, price DECIMAL)");
			// session.execute("CREATE TABLE java_api.summaries (product INT
			// PRIMARY KEY, summary DECIMAL)");
		}

		final Function2<List<String>, Optional<String>, Optional<String>> updateFunction = new Function2<List<String>, Optional<String>, Optional<String>>() {
			// @Override
			public Optional<String> call(List<String> values, Optional<String> state) {
				String newSum = state.or("");
				for (String value : values) {
					newSum += value;

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
				// DayOfWeek,FlightDate,UniqueCarrier,FlightNum,Origin,Dest,DepTime
				// DepDelay DepDelayMinutes DepDel15 ArrTime ArrDelay
				// ArrDelayMinutes ArrDel15

				// 0 1 2 3 4 5 6 7 8 9 10 11 12 13

				return Lists.newArrayList(fields[4] + ":" + fields[5] + ":" + fields[1] + ":" + fields[6] + ":"
						+ fields[2] + " " + fields[3] + ":" + fields[12]);
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
		JavaPairDStream<String, String> flyflow = airport.mapToPair(new PairFunction<String, String, String>() {
			// @Override
			public Tuple2<String, String> call(String s) {
				String[] fields = s.split(":");

//				return new Tuple2<String, String>(
//						fields[0] + ":" + fields[1] + ":" + fields[2] + ":" + fields[3] + ":" + fields[4], s);
				
				return new Tuple2<String, String>(s, s);


			}
		}).updateStateByKey(updateFunction);

		JavaPairDStream<String, String> firstsectionfly = flyflow
				.mapToPair(new PairFunction<Tuple2<String, String>, String, String>() {
					// @Override
					public Tuple2<String, String> call(Tuple2<String, String> s) {
						String[] fields = s._2().split(":");
						String[] DateDecode = fields[2].split("-");
						Calendar FlyDate = Calendar.getInstance();
						FlyDate.set(Integer.parseInt(DateDecode[0]), Integer.parseInt(DateDecode[1]),
								Integer.parseInt(DateDecode[2]));
						FlyDate.set(Calendar.HOUR_OF_DAY, 0);
						FlyDate.set(Calendar.MINUTE, 0);
						FlyDate.set(Calendar.SECOND, 0);
						FlyDate.set(Calendar.MILLISECOND, 0);
						FlyDate.add(Calendar.DAY_OF_MONTH, 2); // Add two day
																// for the
																// return trip

						// String KeyDate =
						// String.valueOf(FlyDate.get(Calendar.YEAR)) +
						// "-"
						// + String.valueOf(FlyDate.get(Calendar.MONTH)) + "-"
						// + String.valueOf(FlyDate.get(Calendar.DAY_OF_MONTH));
						String KeyDate = String.valueOf(FlyDate.get(Calendar.YEAR)) + "-"
								+ String.format("%02d", FlyDate.get(Calendar.MONTH)) + "-"
								+ String.format("%02d", FlyDate.get(Calendar.DAY_OF_MONTH));

						// if (Integer.parseInt(fields[3]) < 1200) {
						// context.write(new Text(element[0]), new
						// Text("travelpart(beg)," + element[1]));
						// context.write(key, new Text("travelpart(beg)," +
						// workingFly));

						String k = fields[1] + ":" + KeyDate;

						return new Tuple2<String, String>(k, s._2());
						// context.write(new Text(fields[7]), new
						// Text("travelpart(beg)," + Fly));
						// }
						// if (Integer.parseInt(fields[4]) > 1200) {
						// // context.write(new Text(element[0]), new
						// // Text("travelpart(end)," + element[1]));
						// // context.write(key, new Text("travelpart(end)," +
						// // workingFly));
						// context.write(new Text(fields[7]), new
						// Text("travelpart(end)," + Fly));
						// }

					}
				}).filter(new Function<Tuple2<String, String>, Boolean>() {
					public Boolean call(Tuple2<String, String> flow) {
						String[] fields = flow._2().split(":");
						return Integer.parseInt(fields[3]) < 1200;
					}
				});

		// firstsectionfly.print();

		JavaPairDStream<String, String> secondsectionfly = flyflow
				.mapToPair(new PairFunction<Tuple2<String, String>, String, String>() {
					// @Override
					public Tuple2<String, String> call(Tuple2<String, String> s) {
						String[] fields = s._2().split(":");
						// String[] DateDecode = fields[2].split("-");
						// Calendar FlyDate = Calendar.getInstance();
						// FlyDate.set(Integer.parseInt(DateDecode[0]),
						// Integer.parseInt(DateDecode[1]),
						// Integer.parseInt(DateDecode[2]));
						// FlyDate.set(Calendar.HOUR_OF_DAY, 0);
						// FlyDate.set(Calendar.MINUTE, 0);
						// FlyDate.set(Calendar.SECOND, 0);
						// FlyDate.set(Calendar.MILLISECOND, 0);
						// FlyDate.add(Calendar.DAY_OF_MONTH, 2); // Add two day
						// for the
						// // return trip
						//
						// // String KeyDate =
						// String.valueOf(FlyDate.get(Calendar.YEAR)) +
						// // "-"
						// // + String.valueOf(FlyDate.get(Calendar.MONTH)) +
						// "-"
						// // +
						// String.valueOf(FlyDate.get(Calendar.DAY_OF_MONTH));
						// String KeyDate =
						// String.valueOf(FlyDate.get(Calendar.YEAR)) + "-"
						// + String.format("%02d", FlyDate.get(Calendar.MONTH))
						// + "-"
						// + String.format("%02d",
						// FlyDate.get(Calendar.DAY_OF_MONTH));
						//

						// if (Integer.parseInt(fields[3]) > 1200) {
						// context.write(new Text(element[0]), new
						// Text("travelpart(beg)," + element[1]));
						// context.write(key, new Text("travelpart(beg)," +
						// workingFly));

						String k = fields[0] + ":" + fields[2];

						return new Tuple2<String, String>(k, s._2());
						// context.write(new Text(fields[7]), new
						// Text("travelpart(beg)," + Fly));
						// }
						// if (Integer.parseInt(fields[4]) > 1200) {
						// // context.write(new Text(element[0]), new
						// // Text("travelpart(end)," + element[1]));
						// // context.write(key, new Text("travelpart(end)," +
						// // workingFly));
						// context.write(new Text(fields[7]), new
						// Text("travelpart(end)," + Fly));
						// }

					}
				}).filter(new Function<Tuple2<String, String>, Boolean>() {
					public Boolean call(Tuple2<String, String> flow) {
						String[] fields = flow._2().split(":");
						return Integer.parseInt(fields[3]) > 1200;
					}
				});
		JavaPairDStream<String, Tuple2<String, String>> trav = firstsectionfly.join(secondsectionfly);
		// trav.print();
		// JavaPairDStream<String, String> validfly = trav
		// .mapToPair(new PairFunction<Tuple2<String, Tuple2<String, String>>,
		// String, String>() {
		// @Override
		// public Tuple2<String, String> call(Tuple2<String, Tuple2<String,
		// String>> flymatching) {
		// String Leg1 = flymatching._2()._1();
		// String[] Leg1Fields = Leg1.split(":");
		// String Leg2 = flymatching._2()._2();
		// String[] Leg2Fields = Leg2.split(":");
		// // DayOfWeek,FlightDate,UniqueCarrier,FlightNum,Origin,Dest,DepTime
		// // DepDelay DepDelayMinutes DepDel15 ArrTime ArrDelay
		// // ArrDelayMinutes ArrDel15
		// // 0 1 2 3 4 5 6 7 8 9 10 11 12 13
		// // Lists.newArrayList(fields[4] + ":" + fields[5] + ":"+
		// // fields[1] + ":" +fields[6]+ ":"+ fields[2] +" "
		// // +fields[3]+":" + fields[12]);
		// String Key = Leg1Fields[0] + ":" + Leg1Fields[1] + ":" +
		// Leg2Fields[1];
		// String Value = Leg1Fields[2] + ":" + Leg1Fields[3] + ":" +
		// Leg1Fields[4] + ":" + Leg1Fields[5]
		// + "|" + Leg2Fields[2] + ":" + Leg2Fields[3] + ":" + Leg2Fields[4] +
		// ":" + Leg2Fields[5];
		//
		// return new Tuple2<String, String>(Key, Value);
		// }
		// });

		JavaDStream<Travel> validfly = trav.map(new Function<Tuple2<String, Tuple2<String, String>>, Travel>() {
			@Override
			public Travel call(Tuple2<String, Tuple2<String, String>> flymatching) {
				String Leg1 = flymatching._2()._1();
				String[] Leg1Fields = Leg1.split(":");
				String Leg2 = flymatching._2()._2();
				String[] Leg2Fields = Leg2.split(":");
				// DayOfWeek,FlightDate,UniqueCarrier,FlightNum,Origin,Dest,DepTime
				// DepDelay DepDelayMinutes DepDel15 ArrTime ArrDelay
				// ArrDelayMinutes ArrDel15
				// 0 1 2 3 4 5 6 7 8 9 10 11 12 13
				// Lists.newArrayList(fields[4] + ":" + fields[5] + ":"+
				// fields[1] + ":" +fields[6]+ ":"+ fields[2] +" "
				// +fields[3]+":" + fields[12]);
		//		String Key = Leg1Fields[0] + ":" + Leg1Fields[1] + ":" + Leg2Fields[1];
		//		String Value = Leg1Fields[2] + ":" + Leg1Fields[3] + ":" + Leg1Fields[4] + ":" + Leg1Fields[5] + "|"
		//				+ Leg2Fields[2] + ":" + Leg2Fields[3] + ":" + Leg2Fields[4] + ":" + Leg2Fields[5];

				return new Travel(Leg1Fields[0], Leg1Fields[1], Leg2Fields[1], Leg1Fields[2],
						Leg1Fields[3],Leg1Fields[4], Float.parseFloat(Leg1Fields[5]), Leg2Fields[2], Leg2Fields[3], Leg2Fields[4],
						Float.parseFloat(Leg2Fields[5]));
			}
		});

		//validfly.print();
		
		validfly.foreachRDD(new Function<JavaRDD<Travel>, Void>() {
			@Override
			public Void call(JavaRDD<Travel> rdd) {
			
				 javaFunctions(rdd).writerBuilder("travel","table1",mapToRow(Travel.class) ).saveToCassandra();

				// javaFunctions(rdd)).saveToCassandra("topcarrierinairport",
				// "table1",mapToRow(AirportCarrier.class));
				return null;
			}
		});
		// secondsectionfly.print();

		// delaycount.print();

		// JavaPairDStream<Float, String> airportcarrierrank = flyflow
		// .mapToPair(new PairFunction<Tuple2<String, MeanDelay>, Float,
		// String>() {
		// @Override
		// public Tuple2<Float, String> call(Tuple2<String, MeanDelay>
		// airportlist) {
		// return new Tuple2<>(new Float(airportlist._2().avg()),
		// airportlist._1());
		// }
		// });

		// JavaPairDStream<Float, String> topairportcarrier = airportcarrierrank
		// .transformToPair(new Function<JavaPairRDD<Float, String>,
		// JavaPairRDD<Float, String>>() {
		// @Override
		// public JavaPairRDD<Float, String> call(JavaPairRDD<Float, String>
		// sortedairport) {
		//
		// //
		// sortedairport.sortByKey(false).saveAsTextFile("hdfs://master:54310/G1-Q1");
		//
		// return sortedairport.sortByKey(false);
		//
		// // CassandraJavaUtil.javaFunctions(sc.makeRDD(Arrays.asList(tuple)))
		// // .writerBuilder("cassandra_java_util_spec",
		// // "test_table_4", mapTupleToRow(
		// // String.class,
		// // Integer.class,
		// // Double.class
		// // )).withColumnSelector(someColumns("stringCol",
		// // "intCol", "doubleCol"))
		// // .saveToCassandra();
		// }
		// });
		// topairportcarrier.print();

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

		// JavaDStream<AirportToAirportArrival> results = airportcarrierrank
		// .map(new Function<Tuple2<Float, String>, AirportToAirportArrival>() {
		// @Override
		// public AirportToAirportArrival call(Tuple2<Float, String> aclist) {
		// String[] key = aclist._2().split(":");
		// return new AirportToAirportArrival(key[0], key[1], aclist._1());
		// }
		//
		// });
		//
		// results.print();
		// results.foreachRDD(new Function<JavaRDD<AirportToAirportArrival>,
		// Void>() {
		// @Override
		// public Void call(JavaRDD<AirportToAirportArrival> rdd) {
		//
		// javaFunctions(rdd).writerBuilder("origindestinationdelay","table1",mapToRow(AirportToAirportArrival.class)
		// ).saveToCassandra();
		//
		// // javaFunctions(rdd)).saveToCassandra("topcarrierinairport",
		// // "table1",mapToRow(AirportCarrier.class));
		// return null;
		// }
		// });
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

	public static class Travel implements Serializable, Comparable {
		// ( airport TEXT, carrier TEXT, departureperf float, PRIMARY KEY
		// ((airport),departureperf) )");
		private String origin;
		private String destination;
		private String stopby;
		private String date;
		private String xydeptime;
		private String xyflight;
		private Float xydelay;
		private String yzdate;
		private String yzdeptime;
		private String yzflight;
		private Float yzdelay;

		// ( origin TEXT, destination TEXT, stopby TEXT, date TEXT, xydeptime
		// TEXT, , xyflight TEXT, xydelay FLOAT, yzdate TEXT, yzdeptime TEXT,
		// yzflight TEXT, yzdelay FLOAT , PRIMARY KEY
		// ((origin,destination,stopby),date))

		public Travel(String origin, String destination, String stopby, String date, String xydeptime, String xyflight,
				Float xydelay, String yzdate, String yzdeptime, String yzflight, Float yzdelay) {
			this.origin = origin;
			this.destination = destination;
			this.stopby = stopby;
			this.date = date;
			this.xydeptime = xydeptime;
			this.xyflight = xyflight;
			this.xydelay = xydelay;
			this.yzdate = yzdate;
			this.yzdeptime = yzdeptime;
			this.yzflight = yzflight;
			this.yzdelay = yzdelay;

		}

		public Travel() {

		}

		public static Travel newInstance(String origin, String destination, String stopby, String date,
				String xydeptime, String xyflight, Float xydelay, String yzdate, String yzdeptime, String yzflight,
				Float yzdelay) {
			Travel travel = new Travel();
			travel.setOrigin(origin);
			travel.setDestination(destination);
			travel.setStopby(stopby);
			travel.setDate(date);
			travel.setXydeptime(xydeptime);
			travel.setXyflight(xyflight);
			travel.setXydelay(xydelay);
			travel.setYzdate(yzdate);
			travel.setYzdeptime(yzdeptime);
			travel.setYzflight(yzflight);
			travel.setYzdelay(yzdelay);

			return travel;
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

		public String getStopby() {
			return stopby;
		}

		public void setStopby(String airport) {
			this.stopby = airport;
		}

		public String getDate() {
			return date;
		}

		public void setDate(String s) {
			this.date = s;
		}

		public String getXyflight() {
			return xyflight;
		}

		public void setXyflight(String s) {
			this.xyflight = s;
		}

		public String getXydeptime() {
			return xydeptime;
		}

		public void setXydeptime(String s) {
			this.xydeptime = s;
		}

		public Float getXydelay() {
			return xydelay;
		}

		public void setXydelay(Float delay) {
			this.xydelay = delay;
		}

		public String getYzdate() {
			return yzdate;
		}

		public void setYzdate(String s) {
			this.yzdate = s;
		}

		public String getYzflight() {
			return yzflight;
		}

		public void setYzflight(String s) {
			this.yzflight = s;
		}

		public String getYzdeptime() {
			return yzdeptime;
		}

		public void setYzdeptime(String s) {
			this.yzdeptime = s;
		}

		public Float getYzdelay() {
			return yzdelay;
		}

		public void setYzdelay(Float delay) {
			this.yzdelay = delay;
		}

		@Override
		public String toString() {
			return Objects.toStringHelper(this).add("origin", origin).add("stopby", stopby)
					.add("destination", destination).add("date", date).add("xydeptime", xydeptime)
					.add("xyflight", xyflight).add("xydelay", xydelay).add("yzdate", yzdate).add("yzdeptime", yzdeptime)
					.add("yzfligth", yzflight).add("yzdelay", yzdelay).toString();
		}

		@Override
		public int compareTo(Object obj) {
			Travel o = (Travel) obj;

			int cmp = o == null ? 1 : this.xydelay.compareTo(o.xydelay);
			return cmp == 0 ? this.yzdelay.compareTo(o.yzdelay) : cmp;
			// return cmp == 0 ? (this.deptime).compareTo(o.carrier) : cmp;
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
