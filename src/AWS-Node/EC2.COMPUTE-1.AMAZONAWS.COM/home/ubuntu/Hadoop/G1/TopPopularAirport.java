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


public class TopPopularAirport extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new TopPopularAirport(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        FileSystem fs = FileSystem.get(conf);
        Path tmpPath = new Path("/coursera/tmp");
        fs.delete(tmpPath, true);

        Job jobCount = Job.getInstance(conf, "Count Fly");
        
        jobCount.setOutputKeyClass(Text.class);
        jobCount.setOutputValueClass(IntWritable.class);

        jobCount.setMapperClass(CountAirportMap.class);
        jobCount.setReducerClass(CountAirportReduce.class);

		TextInputFormat.setInputPaths(jobCount, new Path(args[0]));
		TextOutputFormat.setOutputPath(jobCount, tmpPath);

		jobCount.setJarByClass(TopPopularAirport.class);
		jobCount.waitForCompletion(true);
		

        Job jobTop = Job.getInstance(conf, "Top Popular Airport");
        jobTop.setOutputKeyClass(Text.class);
        jobTop.setOutputValueClass(IntWritable.class);

        jobTop.setMapOutputKeyClass(NullWritable.class);
        jobTop.setMapOutputValueClass(TextArrayWritable.class);

        jobTop.setMapperClass(TopAirportMap.class);
        jobTop.setReducerClass(TopAirportReduce.class);
        jobTop.setNumReduceTasks(1);

        FileInputFormat.setInputPaths(jobTop, tmpPath);
        FileOutputFormat.setOutputPath(jobTop, new Path(args[1]));

        jobTop.setInputFormatClass(KeyValueTextInputFormat.class);
        jobTop.setOutputFormatClass(TextOutputFormat.class);

        jobTop.setJarByClass(TopPopularAirport.class);
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


    
  //  public class PopularAirport {
    	public static class CountAirportMap extends Mapper<Object, Text, Text, IntWritable> {
    		@Override
 
    		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
    			String[] fields = value.toString().replace("\"", "").split((","));//remove the "" on the string that lead to side effect of changing the parsing using ,
    			// StringTokenizer tokenizer = new StringTokenizer(line);
    			// while (tokenizer.hasMoreTokens()) {
    			// String nextToken = tokenizer.nextToken();
    			// context.write(new Text(nextToken), new IntWritable(1));
    			// }
    			if (fields.length > 43) { // this field indicate a fly been canceled
    				if (!fields[0].equals("Year")) {// skip the first line of each file as contain header
    					// if (Integer.parseInt(field[48])==0 )
    					if (!fields[43].isEmpty() && Float.parseFloat(fields[43])==0.00) {
    					

    						if (!fields[11].isEmpty())//if Origin is empty for any reason then line is rejected
    						{
    						context.write(new Text(fields[11]), new IntWritable(1));
    						}else
    						{
    							//System.out.print("Data\n");
    							//for (int i = 0;i< field.length;i++)
        						//{
        						//System.out.print(i+" =>"+field[i]);
        						//System.out.print("\n");
        						//}
    						}
    						if (!fields[18].isEmpty())//if Destination is empty for any reason then line is rejected
    						{
    						context.write(new Text(fields[18]), new IntWritable(1));
    						}
    						else
    						{
    							//System.out.print("Data\n");
    							//for (int i = 0;i< field.length;i++)
        						//{
        						//System.out.print(i+" =>"+field[i]);
        						//System.out.print("\n");
        						//}
    						}
    					}
    				}
    				else
    				{
//    					for (int i = 0;i< field.length;i++)
//    					{
//    					System.out.print(i+" =>"+field[i]);
//    					System.out.print("\n");
//    					}
    				}
    			}
    		}
    	}

    	public static class CountAirportReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
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
    
    
    
    
    
    
 

    public static class TopAirportMap extends Mapper<Text, Text, NullWritable, TextArrayWritable> {
        Integer N;
        private TreeSet<PairLargerFirst<Integer, String>> countToAirportMap = new TreeSet<PairLargerFirst<Integer, String>>();

        @Override
        protected void setup(Context context) throws IOException,InterruptedException {
            Configuration conf = context.getConfiguration();
            this.N = conf.getInt("N", 10);
        }

        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
           
            Integer count = Integer.parseInt(value.toString());
            String AirPortCode = key.toString();

            countToAirportMap.add(new PairLargerFirst<Integer, String>(count, AirPortCode));

            if (countToAirportMap.size() > this.N) {
            	countToAirportMap.remove(countToAirportMap.last());
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
        	for (PairLargerFirst<Integer, String> item : countToAirportMap) {
                String[] strings = {item.second, item.first.toString()};
                TextArrayWritable val = new TextArrayWritable(strings);
                context.write(NullWritable.get(), val);
            }
        }
    }

    public static class TopAirportReduce extends Reducer<NullWritable, TextArrayWritable, Text, IntWritable> {
        Integer N;
        private TreeSet<PairLargerFirst<Integer, String>> countToAirportMap = new TreeSet<PairLargerFirst<Integer, String>>();

        @Override
        protected void setup(Context context) throws IOException,InterruptedException {
            Configuration conf = context.getConfiguration();
            this.N = conf.getInt("N", 10);
        }

        @Override
        public void reduce(NullWritable key, Iterable<TextArrayWritable> values, Context context) throws IOException, InterruptedException {
            for (TextArrayWritable val: values) {
                Text[] pair= (Text[]) val.toArray();

                String word = pair[0].toString();
                Integer count = Integer.parseInt(pair[1].toString());

                countToAirportMap.add(new PairLargerFirst<Integer, String>(count, word));

                if (countToAirportMap.size() > this.N) {
                	countToAirportMap.remove(countToAirportMap.last());
                }
            }

            for (PairLargerFirst<Integer, String> item: countToAirportMap) {
                Text word = new Text(item.second);
                IntWritable value = new IntWritable(item.first);
                context.write(word, value);
            }
        }
    }

}

// From the Class
class Pair<A extends Comparable<? super A>,
        B extends Comparable<? super B>>
        implements Comparable<Pair<A, B>> {

    public final A first;
    public final B second;

    public Pair(A first, B second) {
        this.first = first;
        this.second = second;
    }

    public static <A extends Comparable<? super A>,
            B extends Comparable<? super B>>
    Pair<A, B> of(A first, B second) {
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
        return equal(first, ((Pair<?, ?>) obj).first)
                && equal(second, ((Pair<?, ?>) obj).second);
    }

    private boolean equal(Object o1, Object o2) {
        return o1 == o2 || (o1 != null && o1.equals(o2));
    }

    @Override
    public String toString() {
        return "(" + first + ", " + second + ')';
    }
}


class PairLargerFirst<A extends Comparable<? super A>,
B extends Comparable<? super B>>
implements Comparable<PairLargerFirst<A, B>> {

public final A first;
public final B second;

public PairLargerFirst(A first, B second) {
this.first = first;
this.second = second;
}

public static <A extends Comparable<? super A>,
    B extends Comparable<? super B>>
PairLargerFirst<A, B> of(A first, B second) {
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
return equal(first, ((PairLargerFirst<?, ?>) obj).first)
        && equal(second, ((PairLargerFirst<?, ?>) obj).second);
}

private boolean equal(Object o1, Object o2) {
return o1 == o2 || (o1 != null && o1.equals(o2));
}

@Override
public String toString() {
return "(" + first + ", " + second + ')';
}
}

