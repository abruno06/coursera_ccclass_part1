import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
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
//for Cassandra

//import org.apache.cassandra.hadoop.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.cassandra.thrift.*;
//import org.apache.cassandra.hadoop.cql3.ColumnFamilyOutputFormat;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.cassandra.hadoop.cql3.CqlConfigHelper;
import org.apache.cassandra.hadoop.cql3.CqlOutputFormat;
import org.apache.cassandra.utils.ByteBufferUtil;
//import org.apache.cassandra.thrift.*;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;
import java.util.TreeSet;
import java.util.HashMap;
import java.util.Set;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Map;
import java.util.LinkedHashMap;

public class AirportCarrierDeparturePerformance extends Configured implements Tool {

	static final String KEYSPACE = "group2";

	static final String OUTPUT_COLUMN_FAMILY = "question1";

	private static final String PRIMARY_KEY = "row_key";

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new AirportCarrierDeparturePerformance(), args);
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = this.getConf();
		FileSystem fs = FileSystem.get(conf);
		Path tmpPath = new Path("/coursera/tmp");
		fs.delete(tmpPath, true);

		Job jobSplit = Job.getInstance(conf, "Extract Departure Delay for Airport-Carrier");

		jobSplit.setOutputKeyClass(Text.class);
		jobSplit.setOutputValueClass(FloatWritable.class);

		jobSplit.setMapperClass(CountAirportCarrierMap.class);
		jobSplit.setReducerClass(CountAirportCarrierReduce.class);

		TextInputFormat.setInputPaths(jobSplit, new Path(args[0]));
		TextOutputFormat.setOutputPath(jobSplit, tmpPath);

		jobSplit.setJarByClass(AirportCarrierDeparturePerformance.class);
		jobSplit.waitForCompletion(true);

		Job jobTop = Job.getInstance(conf, "Top for each Airport - Carrier Pair");
		jobTop.setOutputKeyClass(Text.class);
		jobTop.setOutputValueClass(FloatWritable.class);

		jobTop.setMapOutputKeyClass(NullWritable.class);
		jobTop.setMapOutputValueClass(TextArrayWritable.class);

		jobTop.setMapperClass(TopAirportCarrierMap.class);
		jobTop.setReducerClass(TopAirportCarrierReduceToCassandra.class);
		jobTop.setNumReduceTasks(1);

		FileInputFormat.setInputPaths(jobTop, tmpPath);
		// for HDFS
		// jobTop.setOutputKeyClass(Text.class);
		// jobTop.setOutputValueClass(FloatWritable.class);
		// jobTop.setInputFormatClass(KeyValueTextInputFormat.class);
		// FileOutputFormat.setOutputPath(jobTop, new Path(args[1]));
		// For Cassandra
		jobTop.setOutputKeyClass(Map.class);
		jobTop.setOutputValueClass(List.class);
		jobTop.setInputFormatClass(KeyValueTextInputFormat.class);
		ConfigHelper.setOutputInitialAddress(jobTop.getConfiguration(), "localhost");
		// ConfigHelper.setOutputRpcPort(jobTop.getConfiguration(), "9160");
		// ConfigHelper.setOutputKeyspace(jobTop.getConfiguration(),"group2");
		ConfigHelper.setOutputColumnFamily(jobTop.getConfiguration(), KEYSPACE, OUTPUT_COLUMN_FAMILY);

		// String query ="INSERT INTO group2.question1 (airport, carrier,
		// departureperf ) VALUES (? ,? ,? );";
		// String query ="UPDATE group2.question1 SET departurepref = ? ";
		jobTop.getConfiguration().set(PRIMARY_KEY, "airport,departureperf");
		// String query = "INSERT INTO " + KEYSPACE + "." + OUTPUT_COLUMN_FAMILY
		// +
		// " (row_id1, row_id2, airport, carrier, departureperf ) " +
		// " values (?, ?, ?, ?, ?)";
		// String query = "UPDATE " + KEYSPACE + "." + OUTPUT_COLUMN_FAMILY +
		// " SET airport = ? , carrier = ? , departureperf = ? ";;

		String query = "UPDATE " + KEYSPACE + "." + OUTPUT_COLUMN_FAMILY + " SET carrier = ? ";
		CqlConfigHelper.setOutputCql(jobTop.getConfiguration(), query);
		ConfigHelper.setOutputPartitioner(jobTop.getConfiguration(), "Murmur3Partitioner");

		jobTop.setOutputFormatClass(CqlOutputFormat.class);

		// CqlConfigHelper.setInputCQLPageRowSize(job.getConfiguration(), "3");

		jobTop.setJarByClass(AirportCarrierDeparturePerformance.class);
		return jobTop.waitForCompletion(true) ? 0 : 1;
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
	public static class CountAirportCarrierMap extends Mapper<Object, Text, Text, FloatWritable> {
		@Override

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		//	String[] fields = value.toString().replace("\"", "").split((","));
			String[] fields = value.toString().split(",");
			// remove the "" on the string that lead to side effect of changing
			// the parsing using , delimiter

			// StringTokenizer tokenizer = new StringTokenizer(line);
			// while (tokenizer.hasMoreTokens()) {
			// String nextToken = tokenizer.nextToken();
			// context.write(new Text(nextToken), new IntWritable(1));
			// }
		
			//'FlightDate','DayOfWeek','TailNum','FlightNum','DepTime','ArrTime','UniqueCarrier','Origin','Dest','DepDelay','ArrDelay','DepDelayMinutes','ArrDelayMinutes'
		    		
						// if UniCarrier and Delay are empty for any reason then
						// line is rejected
						if (!fields[7].isEmpty() && !fields[6].isEmpty() && !fields[12].isEmpty())

						{
							context.write(
									new Text(new StringBuilder(fields[7]).append(":").append(fields[6]).toString()),
									new FloatWritable(Float.parseFloat(fields[12])));
						} else {
							// System.out.print("Data\n");
							// for (int i = 0;i< field.length;i++)
							// {
							// System.out.print(i+" =>"+field[i]);
							// System.out.print("\n");
							// }
						}

			
		
			
		}
	}

	public static class CountAirportCarrierReduce extends Reducer<Text, FloatWritable, Text, FloatWritable> {
		@Override
		public void reduce(Text key, Iterable<FloatWritable> values, Context context)
				throws IOException, InterruptedException {
			float sum = 0;
			for (FloatWritable val : values) {
				sum += val.get();
			}
			context.write(key, new FloatWritable(sum));
		}
	}

	public static class TopAirportCarrierMap extends Mapper<Text, Text, NullWritable, TextArrayWritable> {
		Integer N;
		// private TreeSet<Pair<Float, String>> countToAirportMap = new
		// TreeSet<Pair<Float, String>>();
		private HashMap<String, TreeSet<Pair<Float, String>>> countByAirportCarrierDepMap = new HashMap<String, TreeSet<Pair<Float, String>>>();

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			this.N = conf.getInt("N", 10);
		}

		@Override
		public void map(Text key, Text value, Context context) throws IOException, InterruptedException {

			Float count = Float.parseFloat(value.toString());
			String[] AirPortCarrierCode = key.toString().split(":");// decouple
																	// the key
			String AirPortCode = AirPortCarrierCode[0];
			String CarrierCode = AirPortCarrierCode[1];
			if (!countByAirportCarrierDepMap.containsKey(AirPortCode)) {

				countByAirportCarrierDepMap.put(AirPortCode, new TreeSet<Pair<Float, String>>());
			}
			// System.out.println(AirPortCode + ">" + CarrierCode);
			TreeSet<Pair<Float, String>> AirportTreeSet = countByAirportCarrierDepMap.get(AirPortCode);
			AirportTreeSet.add(new Pair<Float, String>(count, CarrierCode));
			if (AirportTreeSet.size() > this.N) {
				AirportTreeSet.remove(AirportTreeSet.last());
			}

			// Get an iterator
			// Iterator i = keys.iterator();
			// // Display elements
			// while(i.hasNext()) {
			// Pair<Float, String> CarrierCount = (Pair<Float, String>)
			// countByAirportCarrierDepMap.get(i.next());

			// System.out.print(me.getKey() + ": ");
			// System.out.println(me.getValue());

		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {

			Set AirPortCode = countByAirportCarrierDepMap.keySet();
			Iterator i = AirPortCode.iterator();
			while (i.hasNext()) {
				Object currentKey = i.next();
				TreeSet<Pair<Float, String>> CarrierCount = countByAirportCarrierDepMap.get(currentKey);
				for (Pair<Float, String> item : CarrierCount) {
					String[] strings = { currentKey.toString() + ":" + item.second, item.first.toString() };
					TextArrayWritable val = new TextArrayWritable(strings);
					context.write(NullWritable.get(), val);
				}
			}

		}
	}

	public static class TopAirportCarrierReduceToCassandra
			extends Reducer<NullWritable, TextArrayWritable, Map<String, ByteBuffer>, List<ByteBuffer>> {
		private HashMap<String, TreeSet<Pair<Float, String>>> countByAirportCarrierDepMap = new HashMap<String, TreeSet<Pair<Float, String>>>();
		Integer N;
		private Map<String, ByteBuffer> Compositekeys;
		private String completeKey = "";

		protected void setup(org.apache.hadoop.mapreduce.Reducer.Context context)
				throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			this.N = conf.getInt("N", 10);
			// Map<String, ByteBuffer> Compositekeys = new
			// LinkedHashMap<String,ByteBuffer>();
			// Compositekeys = new LinkedHashMap<String,ByteBuffer>();

			String[] partitionKeys = context.getConfiguration().get(PRIMARY_KEY).split(",");
			// Compositekeys.put("row_id1",
			// ByteBufferUtil.bytes(partitionKeys[0]));
			// Compositekeys.put("row_id2",
			// ByteBufferUtil.bytes(partitionKeys[1]));

		}

		public void reduce(NullWritable key, Iterable<TextArrayWritable> values, Context context)
				throws IOException, InterruptedException {
			for (TextArrayWritable val : values) {
				Text[] pair = (Text[]) val.toArray();

				String[] words = pair[0].toString().split(":");
				completeKey = pair[0].toString();
				String AirPortCode = words[0];
				String CarrierCode = words[1];

				Float count = Float.parseFloat(pair[1].toString());

				if (!countByAirportCarrierDepMap.containsKey(AirPortCode)) {

					countByAirportCarrierDepMap.put(AirPortCode, new TreeSet<Pair<Float, String>>());
				}
				// System.out.println(AirPortCode + ">" + CarrierCode);
				TreeSet<Pair<Float, String>> AirportTreeSet = countByAirportCarrierDepMap.get(AirPortCode);
				AirportTreeSet.add(new Pair<Float, String>(count, CarrierCode));
				if (AirportTreeSet.size() > this.N) {
					AirportTreeSet.remove(AirportTreeSet.last());
				}

			}

			Set AirPortCode = countByAirportCarrierDepMap.keySet();
			Iterator i = AirPortCode.iterator();
			while (i.hasNext()) {
				Object currentKey = i.next();
				Compositekeys = new LinkedHashMap<String, ByteBuffer>();
				Compositekeys.put("airport", ByteBufferUtil.bytes(currentKey.toString()));

				TreeSet<Pair<Float, String>> CarrierCount = countByAirportCarrierDepMap.get(currentKey);
				for (Pair<Float, String> item : CarrierCount) {
					Compositekeys.put("departureperf", ByteBufferUtil.bytes(item.first.toString()));
					// Compositekeys.put("airport_carrier",ByteBufferUtil.bytes(completeKey));
					// context.write(outputKey,Compositekeys,new
					// FloatWritable(item.first));
					// List<ByteBuffer> variables = new ArrayList<ByteBuffer>();
					// variables.add(ByteBufferUtil.bytes(item.first));
					context.write(Compositekeys,
							getBindVariables(currentKey.toString(), item.first.toString() ,item.second));
				}
			} 

		}

		private List<ByteBuffer> getBindVariables(String airport, String carrier, String perf) {
			List<ByteBuffer> variables = new ArrayList<ByteBuffer>();
			// variables.add(Compositekeys.get("row_id1"));
			// variables.add(Compositekeys.get("row_id2"));
			// variables.add(ByteBufferUtil.bytes(airport));
			// variables.add(ByteBufferUtil.bytes(carrier));
			variables.add(ByteBufferUtil.bytes(perf));
			return variables;
		}

	}

	public static class TopAirportCarrierReduce extends Reducer<NullWritable, TextArrayWritable, Text, FloatWritable> {
		Integer N;
		// private TreeSet<Pair<Float, String>> countToAirportMap = new
		// TreeSet<Pair<Float, String>>();
		private HashMap<String, TreeSet<Pair<Float, String>>> countByAirportCarrierDepMap = new HashMap<String, TreeSet<Pair<Float, String>>>();

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			this.N = conf.getInt("N", 10);
		}

		@Override
		public void reduce(NullWritable key, Iterable<TextArrayWritable> values, Context context)
				throws IOException, InterruptedException {
			for (TextArrayWritable val : values) {
				Text[] pair = (Text[]) val.toArray();

				String[] words = pair[0].toString().split(":");
				String AirPortCode = words[0];
				String CarrierCode = words[1];

				Float count = Float.parseFloat(pair[1].toString());

				if (!countByAirportCarrierDepMap.containsKey(AirPortCode)) {

					countByAirportCarrierDepMap.put(AirPortCode, new TreeSet<Pair<Float, String>>());
				}
				// System.out.println(AirPortCode + ">" + CarrierCode);
				TreeSet<Pair<Float, String>> AirportTreeSet = countByAirportCarrierDepMap.get(AirPortCode);
				AirportTreeSet.add(new Pair<Float, String>(count, CarrierCode));
				if (AirportTreeSet.size() > this.N) {
					AirportTreeSet.remove(AirportTreeSet.last());
				}

			}

			//
			Set AirPortCode = countByAirportCarrierDepMap.keySet();
			Iterator i = AirPortCode.iterator();
			while (i.hasNext()) {
				Object currentKey = i.next();
				TreeSet<Pair<Float, String>> CarrierCount = countByAirportCarrierDepMap.get(currentKey);
				for (Pair<Float, String> item : CarrierCount) {
					Text Compositekeys = new Text(currentKey.toString() + ":" + item.second);
					context.write(Compositekeys, new FloatWritable(item.first));
				}
			}

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

	public static <A extends Comparable<? super A>, B extends Comparable<? super B>> Pair<A, B> of(A first, B second) {
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
