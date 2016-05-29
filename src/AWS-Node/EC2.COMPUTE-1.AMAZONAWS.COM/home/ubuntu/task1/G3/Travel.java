
//import org.apache.commons.logging.Log;
//import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.DataOutput;
import java.io.DataInput;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.TreeSet;
import java.util.Calendar;
import org.apache.cassandra.thrift.*;
//import org.apache.cassandra.hadoop.cql3.ColumnFamilyOutputFormat;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.cassandra.hadoop.cql3.CqlConfigHelper;
import org.apache.cassandra.hadoop.cql3.CqlOutputFormat;
import org.apache.cassandra.utils.ByteBufferUtil;





public class Travel extends Configured implements Tool {
	static final String KEYSPACE = "group3";
	static final String OUTPUT_COLUMN_FAMILY = "question2";
	private static final String PRIMARY_KEY = "row_key";

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new Travel(), args);
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = this.getConf();
		FileSystem fs = FileSystem.get(conf);
		Path tmpPath = new Path("/coursera/tmp");
		fs.delete(tmpPath, true);

		// conf.setCombinerClass(Reduce.class);

		Job jobExtract = Job.getInstance(conf, "Extract Fly and build Travel leg");

		jobExtract.setOutputKeyClass(Text.class);
		jobExtract.setOutputValueClass(Text.class);

		jobExtract.setMapperClass(ExtractTravelMap.class);
		// jobExtract.setCombinerClass(ExtractTravelCombiner.class);
		jobExtract.setReducerClass(ExtractTravelReduce.class);

		TextInputFormat.setInputPaths(jobExtract, new Path(args[0]));
		TextOutputFormat.setOutputPath(jobExtract, tmpPath);
		// TextOutputFormat.setOutputPath(jobExtract, new Path(args[1]));

		jobExtract.setJarByClass(Travel.class);
		jobExtract.waitForCompletion(true);

		Job jobTravel = Job.getInstance(conf, "Get Travel leg and build travel possibility");
		// jobTravel.setOutputKeyClass(Text.class);
		// jobTravel.setOutputValueClass(Text.class);

		// jobTravel.setMapOutputKeyClass(NullWritable.class);
		// jobTravel.setMapOutputValueClass(TextArrayWritable.class);

		jobTravel.setMapOutputKeyClass(Text.class);
		jobTravel.setMapOutputValueClass(Text.class);

		jobTravel.setMapperClass(TravelMap.class);
		// jobTravel.setCombinerClass(TravelCombiner.class);
		// jobTravel.setReducerClass(TravelReduce.class);
		// jobTravel.setNumReduceTasks(1);

		FileInputFormat.setInputPaths(jobTravel, tmpPath);

		// for HDFS

		// FileOutputFormat.setOutputPath(jobTravel, new Path(args[1]));
		// jobTravel.setInputFormatClass(KeyValueTextInputFormat.class);
		// jobTravel.setOutputFormatClass(TextOutputFormat.class);

		// For Cassandra
		jobTravel.setReducerClass(TravelToCassandra.class);
		jobTravel.setOutputKeyClass(Map.class);
		jobTravel.setOutputValueClass(List.class);
		jobTravel.setInputFormatClass(KeyValueTextInputFormat.class);
		ConfigHelper.setOutputInitialAddress(jobTravel.getConfiguration(), "localhost");
		// ConfigHelper.setOutputRpcPort(jobTop.getConfiguration(), "9160");
		// ConfigHelper.setOutputKeyspace(jobTop.getConfiguration(),"group2");
		ConfigHelper.setOutputColumnFamily(jobTravel.getConfiguration(), KEYSPACE, OUTPUT_COLUMN_FAMILY);

		// String query ="INSERT INTO group2.question1 (airport, carrier,
		// departureperf ) VALUES (? ,? ,? );";
		// String query ="UPDATE group2.question1 SET departurepref = ? ";
		jobTravel.getConfiguration().set(PRIMARY_KEY, "origin,destination,stopby,date");
		// String query = "INSERT INTO " + KEYSPACE + "." + OUTPUT_COLUMN_FAMILY
		// +
		// " (row_id1, row_id2, airport, carrier, departureperf ) " +
		// " values (?, ?, ?, ?, ?)";
		// String query = "UPDATE " + KEYSPACE + "." + OUTPUT_COLUMN_FAMILY +
		// " SET airport = ? , carrier = ? , departureperf = ? ";;

	//	String query = "UPDATE " + KEYSPACE + "." + OUTPUT_COLUMN_FAMILY + " SET firstleg = ? , secondleg = ? ";
		String query = "UPDATE " + KEYSPACE + "." + OUTPUT_COLUMN_FAMILY
				+ " SET xydeptime = ? , xyarrtime = ? , xyflight = ? , xydelay = ? , yzdate = ? , yzdeptime = ? , yzarrtime = ? , yzflight = ? , yzdelay = ? ";

		CqlConfigHelper.setOutputCql(jobTravel.getConfiguration(), query);
		ConfigHelper.setOutputPartitioner(jobTravel.getConfiguration(), "Murmur3Partitioner");

		jobTravel.setOutputFormatClass(CqlOutputFormat.class);
		// CREATE KEYSPACE group3 WITH replication = {'class': 'SimpleStrategy',
		// 'replication_factor': 3 };
		// CREATE TABLE group3.question2( origin TEXT, destination TEXT, stopby
		// TEXT, date TEXT, firstleg TEXT, secondleg TEXT, PRIMARY KEY
		// ((origin,destination,stopby),date) );

		// CREATE TABLE group3.question2( origin TEXT, destination TEXT, stopby
		// TEXT, date TEXT, xydeptime TEXT, xyarrtime TEXT, xyflight TEXT,
		// xydelay FLOAT, yzdate TEXT, yzdeptime TEXT, yzarrtime TEXT, yzflight
		// TEXT, yzdelay FLOAT , PRIMARY KEY ((origin,destination,stopby),date)
		// );

		jobTravel.setJarByClass(Travel.class);
		return jobTravel.waitForCompletion(true) ? 0 : 1;
	}

	public static class TextArrayWritable extends ArrayWritable {
		public TextArrayWritable() {
			super(Text.class);
		}

		public TextArrayWritable(String[] strings) {
			super(Text.class);
			Text[] texts = new Text[strings.length];
			for (int i = 0; i < strings.length; i++) {
				texts[i] = new Text(strings[i]);
			}
			set(texts);
		}
	}

	// public class PopularAirport {
	public static class ExtractTravelMap extends Mapper<Object, Text, Text, Text> {
		@Override

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] fields = value.toString().split(",");// remove the "" on
															// the string that
															// lead to side
															// effect of
															// changing the
															// parsing using ,
			// 'FlightDate','DayOfWeek','TailNum','FlightNum','DepTime','ArrTime','UniqueCarrier','Origin','Dest','DepDelay','ArrDelay','DepDelayMinutes','ArrDelayMinutes'
			// This Map will generate two K,V value K will be Origine and
			// Destination as O,D can be either the Leg1 of the Travel or the
			// Fly format :
			// Origin,Dest,UniqueCarrier,FlightDate,DepTime,ArrTime,ArrDelayMinutes
			// Leg2
			String Fly;
			if (fields.length > 12) {
				Fly = fields[7] + "," + fields[8] + "," + fields[6] + " " + fields[3] + "," + fields[0] + ","
						+ fields[4] + "," + fields[5] + "," + fields[12];
			} else {
				Fly = fields[7] + "," + fields[8] + "," + fields[6] + " " + fields[3] + "," + fields[0] + ","
						+ fields[4] + "," + fields[5] + ",0.0";
			}
			if (Integer.parseInt(fields[4]) < 1200) {
				// context.write(new Text(element[0]), new
				// Text("travelpart(beg)," + element[1]));
				// context.write(key, new Text("travelpart(beg)," +
				// workingFly));
				context.write(new Text(fields[7]), new Text("travelpart(beg)," + Fly));
			}
			if (Integer.parseInt(fields[4]) > 1200) {
				// context.write(new Text(element[0]), new
				// Text("travelpart(end)," + element[1]));
				// context.write(key, new Text("travelpart(end)," +
				// workingFly));
				context.write(new Text(fields[7]), new Text("travelpart(end)," + Fly));
			}

			// context.write(new Text(fields[8]), new Text(Fly));
		}
	}

	public static class ExtractTravelCombiner extends Reducer<Text, Text, Text, Text> {
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			int sum = 0;

			// Will generate a Travel Leg

			// ArrayList<String> list = new ArrayList<String>();

			int count = 0;
			while (values.iterator().hasNext()) {
				// Fly format :
				// Origin,Dest,UniqueCarrier,FlightDate,DepTime,ArrTime,ArrDelayMinutes
				String workingFly = values.iterator().next().toString();
				String[] fields = workingFly.split(",");

				// list.add(fields[0] + "," + fields[1] + "|" + workingFly);

				if (fields[0].equals(key.toString())) { // potential start
					if (Integer.parseInt(fields[4]) < 1200) {
						// context.write(new Text(element[0]), new
						// Text("travelpart(beg)," + element[1]));
						context.write(key, new Text("travelpart(beg)," + workingFly));
					}
					if (Integer.parseInt(fields[4]) > 1200) {
						// context.write(new Text(element[0]), new
						// Text("travelpart(end)," + element[1]));
						context.write(key, new Text("travelpart(end)," + workingFly));
					}
				}

			}

			// count = list.size();

			// for (String listVal : list) {
			// String[] element = listVal.split("\\|");
			// // System.out.print(element[0]);
			// // System.out.print("\n");
			// // System.out.print(element[1]);
			// // System.out.print("\n");
			// // String[] flyEndPoint = element[0].split((","));
			// String[] fields = element[1].split(",");
			// if (fields[0].equals(key.toString())) { // potential start
			// if (Integer.parseInt(fields[4]) < 1200) {
			// // context.write(new Text(element[0]), new
			// Text("travelpart(beg)," + element[1]));
			// context.write(key, new Text("travelpart(beg)," + element[1]));
			// }
			// if (Integer.parseInt(fields[4]) > 1200) {
			// // context.write(new Text(element[0]), new
			// Text("travelpart(end)," + element[1]));
			// context.write(key, new Text("travelpart(end)," + element[1]));
			// }
			// }

			// if (fields[1].equals(key.toString())) { // potential end
			// if (Integer.parseInt(fields[4]) > 1200) {
			// context.write(new Text(element[0]), new
			// Text("travelpart(end)," + element[1]));
			// }
			// }
			// context.write(new Text(element[0]), new Text(listVal));
			// }
		}

	}

	public static class ExtractTravelReduce extends Reducer<Text, Text, Text, Text> {
		// private HashMap<String, TreeSet<Pair<Float, String>>> FlyDetails =
		// new HashMap<String, TreeSet<Pair<Float, String>>>();

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			// Will generate HasMap to keep only one fly by a Travel Leg
			// O,D,Date,TravelPart.

			// ArrayList<String> list = new ArrayList<String>();

			HashMap<String, TreeSet<Pair<Float, String>>> FlyDetails = new HashMap<String, TreeSet<Pair<Float, String>>>();

			while (values.iterator().hasNext()) {
				// Fly format :
				// travelpart(beg),Origin,Dest,UniqueCarrier,FlightDate,DepTime,ArrTime,ArrDelayMinutes
				String workingFly = values.iterator().next().toString();
				String[] fields = workingFly.split(",");

				// list.add(fields[0] + "," + fields[1] + "|" + workingFly);

				String FlyKey = fields[1] + "," + fields[2] + "," + fields[4] + "," + fields[0];
				Float delay = new Float(fields[7]);
				if (FlyDetails.containsKey(FlyKey)) {

					FlyDetails.get(FlyKey).add(new Pair<Float, String>(delay, workingFly + "|" + key.toString()));
				} else {
					FlyDetails.put(FlyKey, new TreeSet<Pair<Float, String>>());
					FlyDetails.get(FlyKey).add(new Pair<Float, String>(delay, workingFly + "|" + key.toString()));

				}

			}

			for (TreeSet<Pair<Float, String>> item : FlyDetails.values()) {

				String[] FlyData = item.first().second.split("\\|");

				context.write(new Text(FlyData[1]), new Text(FlyData[0]));

			}

			// for (String listVal : list) {
			// String[] element = listVal.split("\\|");
			// // System.out.print(element[0]);
			// // System.out.print("\n");
			// // System.out.print(element[1]);
			// // System.out.print("\n");
			// // String[] flyEndPoint = element[0].split((","));
			// String[] fields = element[1].split(",");
			// if (fields[0].equals(key.toString())) { // potential start
			// if (Integer.parseInt(fields[4]) < 1200) {
			// context.write(new Text(element[0]), new Text("travelpart(beg)," +
			// element[1]));
			// }
			// if (Integer.parseInt(fields[4]) > 1200) {
			// context.write(new Text(element[0]), new Text("travelpart(end)," +
			// element[1]));
			// }
			// }
			//
			// // if (fields[1].equals(key.toString())) { // potential end
			// // if (Integer.parseInt(fields[4]) > 1200) {
			// // context.write(new Text(element[0]), new
			// // Text("travelpart(end)," + element[1]));
			// // }
			// // }
			// // context.write(new Text(element[0]), new Text(listVal));
			// }

		}

	}

	public static class TravelMap extends Mapper<Text, Text, Text, Text> {
		Integer N;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			this.N = conf.getInt("N", 10);
		}

		@Override
		public void map(Text key, Text value, Context context) throws IOException, InterruptedException {

			String[] fields = value.toString().split(",");// remove the "" on

			// Leg(typ),'FlightDate','DayOfWeek','TailNum','FlightNum','DepTime','ArrTime','UniqueCarrier','Origin','Dest','DepDelay','ArrDelay','DepDelayMinutes','ArrDelayMinutes'
			// This Map will generate two K,V value K will be Origine and
			// Destination as O,D can be either the Leg1 of the Travel or the
			// Leg2
			// String Fly;
			// if (fields.length > 13) {
			// Fly = fields[8] + "," + fields[9] + "," + fields[7] + "," +
			// fields[1] + "," + fields[5] + ","
			// + fields[6] + "," + fields[13];
			// } else {
			// Fly = fields[8] + "," + fields[9] + "," + fields[7] + "," +
			// fields[1] + "," + fields[5] + ","
			// + fields[6] + ",0.0";
			// }
			// ABQ,HOU travelpart(beg),ABQ,HOU,WN,2008-06-05,0638,0939,0.00
			// ABQ,HOU travelpart(end),ABQ,HOU,WN,2008-06-05,2031,2327,52.00
			if (fields[0].equals(new String("travelpart(beg)"))) {
				// Add two days to the Start Date
				Calendar FlyDate = Calendar.getInstance();
				String[] DateDecode = fields[4].split("-");
				FlyDate.set(Integer.parseInt(DateDecode[0]), Integer.parseInt(DateDecode[1]),
						Integer.parseInt(DateDecode[2]));
				FlyDate.set(Calendar.HOUR_OF_DAY, 0);
				FlyDate.set(Calendar.MINUTE, 0);
				FlyDate.set(Calendar.SECOND, 0);
				FlyDate.set(Calendar.MILLISECOND, 0);
				FlyDate.add(Calendar.DAY_OF_MONTH, 2); // Add two day for the
														// return trip

				// String KeyDate = String.valueOf(FlyDate.get(Calendar.YEAR)) +
				// "-"
				// + String.valueOf(FlyDate.get(Calendar.MONTH)) + "-"
				// + String.valueOf(FlyDate.get(Calendar.DAY_OF_MONTH));
				String KeyDate = String.valueOf(FlyDate.get(Calendar.YEAR)) + "-"
						+ String.format("%02d", FlyDate.get(Calendar.MONTH)) + "-"
						+ String.format("%02d", FlyDate.get(Calendar.DAY_OF_MONTH));
				context.write(new Text(fields[2] + "," + KeyDate), value);
			} else {
				context.write(new Text(fields[1] + "," + fields[4]), value);
			}

		}

		// @Override
		// protected void cleanup(Context context) throws IOException,
		// InterruptedException {
		// for (PairLargerFirst<Integer, String> item : countToAirportMap) {
		// String[] strings = { item.second, item.first.toString() };
		// TextArrayWritable val = new TextArrayWritable(strings);
		// context.write(NullWritable.get(), val);
		// }
		// }
	}

	public static class TravelReduce extends Reducer<Text, Text, Text, Text> {
		Integer N;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			this.N = conf.getInt("N", 10);
		}
		// Leg(typ),'FlightDate','DayOfWeek','TailNum','FlightNum','DepTime','ArrTime','UniqueCarrier','Origin','Dest','DepDelay','ArrDelay','DepDelayMinutes','ArrDelayMinutes'

		// ABQ,HOU travelpart(beg),ABQ,HOU,WN,2008-06-05,0638,0939,0.00
		// ABQ,HOU travelpart(end),ABQ,HOU,WN,2008-06-05,2031,2327,52.00
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			// for (Text val : values) {
			// context.write(key, val);
			// }
			ArrayList<String> listPart1 = new ArrayList<String>();
			ArrayList<String> listPart2 = new ArrayList<String>();
			for (Text val : values) {
				String[] fields = val.toString().split(",");
				if (fields[0].equals(new String("travelpart(beg)"))) {
					// context.write(new Text(fields[9]), value);
					listPart1.add(fields[1] + "," + fields[2] + "|" + val);
				} else {
					// context.write(new Text(fields[8]), value);
					listPart2.add(fields[1] + "," + fields[2] + "|" + val);
				}
			}

			for (String Startfly : listPart1) {
				String[] elementStart = Startfly.split("\\|");
				String[] fieldsStart = elementStart[1].split(",");

				for (String Endfly : listPart2) {
					String[] elementEnd = Endfly.split("\\|");
					String[] fieldsEnd = elementEnd[1].split(",");
					Calendar EndDate = Calendar.getInstance();
					String[] EndDateDecode = fieldsEnd[4].split("-");
					context.write(new Text(fieldsStart[1] + "," + fieldsEnd[1] + "," + fieldsEnd[2]),
							new Text(elementStart[0] + "," + elementEnd[0] + "|" + Startfly + "|" + Endfly));

				}

			}

		}
	}

	public static class TravelCombiner extends Reducer<Text, Text, Text, Text> {

		private HashMap<String, TreeSet<Pair<Float, String>>> FlyDelayMap = new HashMap<String, TreeSet<Pair<Float, String>>>();

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			// emit the keys that have the shortest delay

			for (Text val : values) {
				String[] fields = val.toString().split(",");
				float delay = Float.parseFloat(fields[7]);
				// ABE,2008-10-23
				// travelpart(beg),CLT,ABE,YV,2008-10-21,1129,1307,0.00
				// ABE,2008-10-23
				// travelpart(beg),DTW,ABE,9E,2008-10-21,1002,1123,0.00
				String FlyKey = fields[0] + "," + fields[1] + "," + fields[2] + "," + fields[4];
				if (FlyDelayMap.containsKey(FlyKey)) {

					FlyDelayMap.get(FlyKey)
							.add(new Pair<Float, String>(delay, fields.toString() + "|" + key.toString()));
				} else {
					FlyDelayMap.put(FlyKey, new TreeSet<Pair<Float, String>>());
					FlyDelayMap.get(FlyKey)
							.add(new Pair<Float, String>(delay, fields.toString() + "|" + key.toString()));

				}

			}

			// for (Pair<Float, String> item : FlyDelayMap) {
			// Text word = new Text(item.second);
			// FloatWritable value = new FloatWritable(item.first);
			// context.write(word, value);
			// }

			for (TreeSet<Pair<Float, String>> item : FlyDelayMap.values()) {

				String[] FlyData = item.first().second.split("\\|");

				context.write(new Text(FlyData[1]), new Text(FlyData[0]));

			}

		}
	}

	public static class TravelToCassandra extends Reducer<Text, Text, Map<String, ByteBuffer>, List<ByteBuffer>> {
		private HashMap<String, TreeSet<Pair<Float, String>>> TravelMap = new HashMap<String, TreeSet<Pair<Float, String>>>();
		Integer N;

		protected void setup(org.apache.hadoop.mapreduce.Reducer.Context context)
				throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			this.N = conf.getInt("N", 10);

			String[] partitionKeys = context.getConfiguration().get(PRIMARY_KEY).split(",");

		}
		// ABQ,HOU travelpart(beg),ABQ,HOU,WN,2008-06-05,0638,0939,0.00
		// ABQ,HOU travelpart(end),ABQ,HOU,WN,2008-06-05,2031,2327,52.00

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			Map<String, ByteBuffer> Compositekeys;
			// for (TextArrayWritable val : values) {
			// Text[] pair = (Text[]) val.toArray();
			//
			// String[] words = pair[0].toString().split(":");
			// pair[0].toString();
			// String OriginAirportCode = words[0];
			// String DestinationAirportCode = words[1];
			//
			// Float count = Float.parseFloat(pair[1].toString());
			//
			// if (!TravelMap.containsKey(OriginAirportCode)) {
			//
			// TravelMap.put(OriginAirportCode, new TreeSet<Pair<Float,
			// String>>());
			// }
			// // System.out.println(AirPortCode + ">" + CarrierCode);
			// TreeSet<Pair<Float, String>> AirportTreeSet =
			// TravelMap.get(OriginAirportCode);
			// AirportTreeSet.add(new Pair<Float, String>(count,
			// DestinationAirportCode));
			// if (AirportTreeSet.size() > this.N) {
			// AirportTreeSet.remove(AirportTreeSet.last());
			// }
			//
			// }
			//
			// Set AirPortCode = countByAirportAirportDepMap.keySet();
			// Iterator i = AirPortCode.iterator();
			// while (i.hasNext()) {
			// Object currentKey = i.next();
			// Compositekeys = new LinkedHashMap<String, ByteBuffer>();
			// Compositekeys.put("origin",
			// ByteBufferUtil.bytes(currentKey.toString()));
			//
			// TreeSet<Pair<Float, String>> AirportCount =
			// countByAirportAirportDepMap.get(currentKey);
			// for (Pair<Float, String> item : AirportCount) {
			// Compositekeys.put("departureperf",
			// ByteBufferUtil.bytes(item.first));
			// //
			// Compositekeys.put("airport_carrier",ByteBufferUtil.bytes(completeKey));
			// // context.write(outputKey,Compositekeys,new
			// // FloatWritable(item.first));
			// // List<ByteBuffer> variables = new ArrayList<ByteBuffer>();
			// // variables.add(ByteBufferUtil.bytes(item.first));
			// context.write(Compositekeys,
			// getBindVariables(currentKey.toString(), item.second,
			// item.first.toString()));
			// }
			// }
			//
			//

			// ABQ,HOU travelpart(end),ABQ,HOU,WN,2008-06-05,2031,2327,52.00

			// for (Text val : values) {
			// context.write(key, val);
			// }
			ArrayList<String> listPart1 = new ArrayList<String>();
			ArrayList<String> listPart2 = new ArrayList<String>();

			for (Text val : values) {
				String[] fields = val.toString().split(",");
				if (fields[0].equals(new String("travelpart(beg)"))) {
					// context.write(new Text(fields[9]), value);
					listPart1.add(fields[1] + "," + fields[2] + "|" + val);
				} else {
					// context.write(new Text(fields[8]), value);
					listPart2.add(fields[1] + "," + fields[2] + "|" + val);
				}
			}

			for (String Startfly : listPart1) {
				String[] elementStart = Startfly.split("\\|");
				String[] fieldsStart = elementStart[1].split(",");
				Compositekeys = new LinkedHashMap<String, ByteBuffer>();
				Compositekeys.put("origin", ByteBufferUtil.bytes(fieldsStart[1]));

				for (String Endfly : listPart2) {
					String[] elementEnd = Endfly.split("\\|");
					String[] fieldsEnd = elementEnd[1].split(",");
					Calendar EndDate = Calendar.getInstance();
					String[] EndDateDecode = fieldsEnd[4].split("-");

					Compositekeys.put("stopby", ByteBufferUtil.bytes(fieldsEnd[1]));
					Compositekeys.put("destination", ByteBufferUtil.bytes(fieldsEnd[2]));
					Compositekeys.put("date", ByteBufferUtil.bytes(fieldsStart[4]));
					// CREATE TABLE group3.question2( origin TEXT, destination
					// TEXT, stopby TEXT, date TEXT, xydeptime TEXT, xyarrtime
					// TEXT, xyflight TEXT, xydelay FLOAT, yzdate TEXT,
					// yzdeptime TEXT, yzarrtime TEXT, yzflight TEXT, yzdelay
					// FLOAT , PRIMARY KEY ((origin,destination,stopby),date) );

					// to keep 3,4,5,6,7
					context.write(Compositekeys,

							// getBindVariables(String xydeptime, String
							// xyarrtime,String xyflight, Float xydelay, String
							// yzdate ,String yzdeptime, String yzarrtime,String
							// yzflight, Float yzdelay,)
							// ABQ,HOU
							// travelpart(end),ABQ,HOU,WN,2008-06-05,2031,2327,52.00
							// 0 1 2 3 4 5 6 7
							getBindVariables(fieldsStart[5], fieldsStart[6], fieldsStart[3], new Float(fieldsStart[7]),
									fieldsEnd[4], fieldsEnd[5], fieldsEnd[6], fieldsEnd[3], new Float(fieldsEnd[7])));

					// getBindVariables(
					// fieldsStart[3] + "," + fieldsStart[4] + "," +
					// fieldsStart[5] + "," + fieldsStart[6]
					// + "," + fieldsStart[7],
					// fieldsEnd[3] + "," + fieldsEnd[4] + "," + fieldsEnd[5] +
					// "," + fieldsEnd[6] + ","
					// + fieldsEnd[7]));
					// context.write(new Text(fieldsStart[1] + "," +
					// fieldsEnd[1] + "," + fieldsEnd[2]),
					// new Text(elementStart[0] + "," + elementEnd[0] + "|" +
					// Startfly + "|" + Endfly));

				}

			}
		}

		// private List<ByteBuffer> getBindVariables(String FlyStop, String
		// FlyEnd) {
		// List<ByteBuffer> variables = new ArrayList<ByteBuffer>();
		// // variables.add(Compositekeys.get("row_id1"));
		// // variables.add(Compositekeys.get("row_id2"));
		// variables.add(ByteBufferUtil.bytes(FlyStop));
		// variables.add(ByteBufferUtil.bytes(FlyEnd));
		// // variables.add(ByteBufferUtil.bytes(perf));
		// return variables;
		// }

		private List<ByteBuffer> getBindVariables(String xydeptime, String xyarrtime,String xyflight, Float xydelay, String yzdate ,String yzdeptime, String yzarrtime,String yzflight, Float yzdelay) {
			List<ByteBuffer> variables = new ArrayList<ByteBuffer>();
			variables.add(ByteBufferUtil.bytes(xydeptime));
			variables.add(ByteBufferUtil.bytes(xyarrtime));
			variables.add(ByteBufferUtil.bytes(xyflight));
			variables.add(ByteBufferUtil.bytes(xydelay));
			variables.add(ByteBufferUtil.bytes(yzdate));
			variables.add(ByteBufferUtil.bytes(yzdeptime));
			variables.add(ByteBufferUtil.bytes(yzarrtime));
			variables.add(ByteBufferUtil.bytes(yzflight));
			variables.add(ByteBufferUtil.bytes(yzdelay));
			// variables.add(ByteBufferUtil.bytes(perf));
			return variables;
		}


	}

	}

	// From the Class
	class Pair<A extends Comparable<? super A>, B extends Comparable<? super B>> implements Comparable<Pair<A, B>> {

		public final A first;
		public final B second;

		public Pair(A first, B second) {
			this.first = first;
			this.second = second;
		}

		public static <A extends Comparable<? super A>, B extends Comparable<? super B>> Pair<A, B> of(A first,
				B second) {
			return new Pair<A, B>(first, second);
		}

		@Override
		public int compareTo(Pair<A, B> o) {
			int cmp = o == null ? 1 : (this.first).compareTo(o.first);
			return cmp == 0 ? (this.second).compareTo(o.second) : cmp;
		}

		@Override
		public int hashCode() {
			return 31 * hashcode(first) + hashcode(second);
		}

		private static int hashcode(Object o) {
			return o == null ? 0 : o.hashCode();
		}

		@Override
		public boolean equals(Object obj) {
			if (!(obj instanceof Pair))
				return false;
			if (this == obj)
				return true;
			return equal(first, ((Pair<?, ?>) obj).first) && equal(second, ((Pair<?, ?>) obj).second);
		}

		private boolean equal(Object o1, Object o2) {
			return o1 == o2 || (o1 != null && o1.equals(o2));
		}

		@Override
		public String toString() {
			return "(" + first + ", " + second + ')';
		}
	}

class PairLargerFirst<A extends Comparable<? super A>, B extends Comparable<? super B>>
		implements Comparable<PairLargerFirst<A, B>> {

	public final A first;
	public final B second;

	public PairLargerFirst(A first, B second) {
		this.first = first;
		this.second = second;
	}

	public static <A extends Comparable<? super A>, B extends Comparable<? super B>> PairLargerFirst<A, B> of(A first,
			B second) {
		return new PairLargerFirst<A, B>(first, second);
	}

	@Override
	public int compareTo(PairLargerFirst<A, B> o) {
		int cmp = o == null ? 1 : (o.first).compareTo(this.first);
		return cmp == 0 ? (o.second).compareTo(this.second) : cmp;
	} // this is the function reversed compare to the original Pair

	@Override
	public int hashCode() {
		return 31 * hashcode(first) + hashcode(second);
	}

	private static int hashcode(Object o) {
		return o == null ? 0 : o.hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof PairLargerFirst))
			return false;
		if (this == obj)
			return true;
		return equal(first, ((PairLargerFirst<?, ?>) obj).first) && equal(second, ((PairLargerFirst<?, ?>) obj).second);
	}

	private boolean equal(Object o1, Object o2) {
		return o1 == o2 || (o1 != null && o1.equals(o2));
	}

	@Override
	public String toString() {
		return "(" + first + ", " + second + ')';
	}
	
	
}
