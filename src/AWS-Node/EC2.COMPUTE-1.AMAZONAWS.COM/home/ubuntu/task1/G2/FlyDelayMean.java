import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.FloatWritable;
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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.List;
import java.util.StringTokenizer;
import java.util.TreeSet;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.ArrayList;
import java.util.Map;

import org.apache.cassandra.thrift.*;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.cassandra.hadoop.cql3.CqlConfigHelper;
import org.apache.cassandra.hadoop.cql3.CqlOutputFormat;
import org.apache.cassandra.utils.ByteBufferUtil;

public class FlyDelayMean extends Configured implements Tool {
	static final String KEYSPACE = "group2";
	static final String OUTPUT_COLUMN_FAMILY = "question4";
	private static final String PRIMARY_KEY = "row_key";

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new FlyDelayMean(), args);
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = this.getConf();
		FileSystem fs = FileSystem.get(conf);
		Path tmpPath = new Path("/coursera/tmp");
		fs.delete(tmpPath, true);

		Job jobCount = Job.getInstance(conf, "extract Fly Delay for X-Y pair");

//		jobCount.setOutputKeyClass(Text.class);
//		jobCount.setOutputValueClass(FloatWritable.class);

		jobCount.setMapperClass(ComputeFlyDelayMap.class);
//		jobCount.setReducerClass(ComputeFlyDelayReduce.class);
		jobCount.setMapOutputKeyClass(Text.class);
		jobCount.setMapOutputValueClass(FloatWritable.class);
	
		TextInputFormat.setInputPaths(jobCount, new Path(args[0]));
//		TextOutputFormat.setOutputPath(jobCount, new Path(args[1]));


		// FileOutputFormat.setOutputPath(jobTravel, new Path(args[1]));


		// For Cassandra
		jobCount.setReducerClass(ComputeFlyDelayReduceToCassandra.class);
		jobCount.setOutputKeyClass(Map.class);
		jobCount.setOutputValueClass(List.class);
		//jobCount.setInputFormatClass(KeyValueTextInputFormat.class);
		ConfigHelper.setOutputInitialAddress(jobCount.getConfiguration(), "localhost");
		// ConfigHelper.setOutputRpcPort(jobTop.getConfiguration(), "9160");
	
		ConfigHelper.setOutputColumnFamily(jobCount.getConfiguration(), KEYSPACE, OUTPUT_COLUMN_FAMILY);

		
		jobCount.getConfiguration().set(PRIMARY_KEY, "origin,destination");

		String query = "UPDATE " + KEYSPACE + "." + OUTPUT_COLUMN_FAMILY + " SET meanarrivaldelay = ? ";

		CqlConfigHelper.setOutputCql(jobCount.getConfiguration(), query);
		ConfigHelper.setOutputPartitioner(jobCount.getConfiguration(), "Murmur3Partitioner");

		jobCount.setOutputFormatClass(CqlOutputFormat.class);

		
		// CREATE TABLE group2.question4 ( origin TEXT, destination TEXT, meanarrivaldelay FLOAT , PRIMARY KEY ((origin,destination)));	
		
		// Job jobTop = Job.getInstance(conf, "Compute Meanof X-Y Arrival
		// Delay");
		// jobTop.setOutputKeyClass(Text.class);
		// jobTop.setOutputValueClass(FloatWritable.class);
		//
		// jobTop.setMapOutputKeyClass(NullWritable.class);
		// jobTop.setMapOutputValueClass(TextArrayWritable.class);
		//
		// jobTop.setMapperClass(TopOnTimeMap.class);
		// jobTop.setReducerClass(TopOnTimeReduce.class);
		// jobTop.setNumReduceTasks(1);
		//
		// FileInputFormat.setInputPaths(jobTop, tmpPath);
		// FileOutputFormat.setOutputPath(jobTop, new Path(args[1]));
		//
		// jobTop.setInputFormatClass(KeyValueTextInputFormat.class);
		// jobTop.setOutputFormatClass(TextOutputFormat.class);
		//
		// jobTop.setJarByClass(FlyDelayMean.class);

		jobCount.setJarByClass(FlyDelayMean.class);
		
		return jobCount.waitForCompletion(true) ? 0 : 1;
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
	public static class ComputeFlyDelayMap extends Mapper<Object, Text, Text, FloatWritable> {
		@Override

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			//'FlightDate','DayOfWeek','TailNum','FlightNum','DepTime','ArrTime','UniqueCarrier','Origin','Dest','DepDelay','ArrDelay','DepDelayMinutes','ArrDelayMinutes'
			String[] fields = value.toString().split(",");	
		
			
			if (fields.length > 12) {
						context.write(new Text(fields[7]+":"+fields[8]),new FloatWritable(Float.parseFloat(fields[12])));
			}
		//	 System.err.print(fields.length+">"+value.toString()+"\n");
									
			
			// ,
			// StringTokenizer tokenizer = new StringTokenizer(line);
			// while (tokenizer.hasMoreTokens()) {
			// String nextToken = tokenizer.nextToken();
			// context.write(new Text(nextToken), new IntWritable(1));
			// }
			
		
			  
			
//			if (fields.length > 43) { // this field indicate a fly been canceled
//				if (!fields[0].equals("Year")) {// skip the first line of each
//												// file as contain header
//					// if (Integer.parseInt(field[48])==0 )
//					if (!fields[43].isEmpty() && Float.parseFloat(fields[43]) == 0.00) {
//
//						if (!fields[11].isEmpty() && !fields[18].isEmpty() && !fields[38].isEmpty())// if
//																									// UniCarrier
//																									// and
//																									// Delay
//																									// are
//																									// empty
//																									// for
//																									// any
//																									// reason
//																									// then
//																									// line
//																									// is
//																									// rejected
//						{
//
//							context.write(
//									new Text(new StringBuilder(fields[11]).append(":").append(fields[18]).toString()),
//									new FloatWritable(Float.parseFloat(fields[38])));
//						} else {
//							// System.out.print("Data\n");
//							// for (int i = 0;i< field.length;i++)
//							// {
//							// System.out.print(i+" =>"+field[i]);
//							// System.out.print("\n");
//							// }
//						}
//
//					}
//				} else {
//					// for (int i = 0;i< field.length;i++)
//					// {
//					// System.out.print(i+" =>"+field[i]);
//					// System.out.print("\n");
//					// }
//				}
//			}
			
		
		}
	}

	public static class ComputeFlyDelayReduce extends Reducer<Text, FloatWritable, Text, FloatWritable> {
		@Override
		public void reduce(Text key, Iterable<FloatWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			int total = 0;
			for (FloatWritable val : values) {
				sum += val.get();
				total++;
			}
			context.write(key, new FloatWritable(sum / total));
		}
	}

	public static class TopOnTimeMap extends Mapper<Text, Text, NullWritable, TextArrayWritable> {
		Integer N;
		private TreeSet<Pair<Float, String>> delayForAirlineMap = new TreeSet<Pair<Float, String>>();

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			this.N = conf.getInt("N", 10);
		}

		@Override
		public void map(Text key, Text value, Context context) throws IOException, InterruptedException {

			Float count = Float.parseFloat(value.toString());
			String AirlineCode = key.toString();

			delayForAirlineMap.add(new Pair<Float, String>(count, AirlineCode));

			if (delayForAirlineMap.size() > this.N) {
				delayForAirlineMap.remove(delayForAirlineMap.last());
			}
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			for (Pair<Float, String> item : delayForAirlineMap) {
				String[] strings = { item.second, item.first.toString() };
				TextArrayWritable val = new TextArrayWritable(strings);
				context.write(NullWritable.get(), val);
			}
		}
	}

	public static class TopOnTimeReduce extends Reducer<NullWritable, TextArrayWritable, Text, FloatWritable> {
		Integer N;
		private TreeSet<Pair<Float, String>> delayForAirlineMap = new TreeSet<Pair<Float, String>>();

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

				String word = pair[0].toString();
				Float count = Float.parseFloat(pair[1].toString());

				delayForAirlineMap.add(new Pair<Float, String>(count, word));

				if (delayForAirlineMap.size() > this.N) {
					delayForAirlineMap.remove(delayForAirlineMap.last());
				}
			}

			for (Pair<Float, String> item : delayForAirlineMap) {
				Text word = new Text(item.second);
				FloatWritable value = new FloatWritable(item.first);
				context.write(word, value);
			}
		}
	}

	public static class ComputeFlyDelayReduceToCassandra
			extends Reducer<Text, FloatWritable, Map<String, ByteBuffer>, List<ByteBuffer>> {

		public void reduce(Text key, Iterable<FloatWritable> values, Context context)
				throws IOException, InterruptedException {
			Map<String, ByteBuffer> Compositekeys;
			
			
			
			int sum = 0;
			int total = 0;
			for (FloatWritable val : values) {
				sum += val.get();
				total++;
			}
			String [] detail = key.toString().split(":");
			Compositekeys = new LinkedHashMap<String, ByteBuffer>();
			Compositekeys.put("origin", ByteBufferUtil.bytes(detail[0]));
			Compositekeys.put("destination", ByteBufferUtil.bytes(detail[1]));

		//	context.write(key, new FloatWritable(sum / total));
			
			
			
			context.write(Compositekeys,getBindVariables(new Float(sum / total)));

		}
		
		private List<ByteBuffer> getBindVariables(Float meantime) {
			List<ByteBuffer> variables = new ArrayList<ByteBuffer>();
			
			variables.add(ByteBufferUtil.bytes(meantime));
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