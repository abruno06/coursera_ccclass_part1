
//Rank the top 10 most popular airports by numbers of flights to/from the airport

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class TopAirport {
	public static class TopAirportMap extends Mapper<Object, Text, Text, IntWritable> {
		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] field = value.toString().replace("\"", "").split((","));
			// StringTokenizer tokenizer = new StringTokenizer(line);
			// while (tokenizer.hasMoreTokens()) {
			// String nextToken = tokenizer.nextToken();
			// context.write(new Text(nextToken), new IntWritable(1));
			// }
			if (field.length > 43) {
				if (!field[0].equals("Year")) {
					// if (Integer.parseInt(field[48])==0 )
					if (!field[43].isEmpty() && Float.parseFloat(field[43])==0.00) {
						//System.out.print("Data\n");
						for (int i = 0;i< field.length;i++)
						{
						//System.out.print(i+" =>"+field[i]);
						//System.out.print("\n");
						}
						
						context.write(new Text(field[11]), new IntWritable(1));
						context.write(new Text(field[18]), new IntWritable(1));
					}
				}
				else
				{
//					for (int i = 0;i< field.length;i++)
//					{
//					System.out.print(i+" =>"+field[i]);
//					System.out.print("\n");
//					}
				}
			}
		}
	}

	public static class TopAirportReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}

	public static void main(String[] args) throws Exception {

		Job job = Job.getInstance(new Configuration(), "topairport");
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setMapperClass(TopAirportMap.class);
		job.setReducerClass(TopAirportReduce.class);

		TextInputFormat.setInputPaths(job, new Path(args[0]));
		TextOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setJarByClass(TopAirport.class);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}