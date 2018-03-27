package project.chicagocrimes.partitioning;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Partitioning {
	
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "PostCommentHierarchy");
		job.setJarByClass(Partitioning.class);

		job.setMapperClass(Final_partition_date_mapper.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setPartitionerClass(Final_partition_date_partitioner.class);
		Final_partition_date_partitioner.setMinLastAccessDate(job, 2001);
		job.setNumReduceTasks(5);
		job.setReducerClass(Final_partition_date_reducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

	public static class Final_partition_date_mapper extends Mapper<Object, Text, IntWritable, Text> {

		private IntWritable outKey = new IntWritable();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			String[] separatedInput = value.toString().split("\t");

			String crimeString = separatedInput[1];
			if (crimeString == null) {
				return;
			}
			
		    String copyOfCrime = new String();

		    boolean inQuotes = false;

		    for(int i=0; i<crimeString.length(); ++i)
	        {
		        if (crimeString.charAt(i)=='"')
		            inQuotes = !inQuotes;
		        if (crimeString.charAt(i)==',' && inQuotes)
		            copyOfCrime += '|';
		        else
		            copyOfCrime += crimeString.charAt(i);
	        }
			
			String[] separatedCrime = copyOfCrime.toString().split(",");
			String year = separatedCrime[18];
			if (year == null) {
				return;
			}
			
			
			if (year.toString().contains("Year")) /* Some condition satisfying it is header */
				return;
			else {
				try {
					outKey.set(Integer.parseInt(year));
					
				} catch (Exception e) {
					// TODO: handle exception
				}
				context.write(outKey, value);
			}
		}
	}

	public static class Final_partition_date_partitioner extends Partitioner<IntWritable, Text>
			implements Configurable {
		private static final String MIN_LAST_ACCESS_DATE_MONTH = "min.last.access.date.year";
		private Configuration conf = null;
		private static int minLastAccessDateMonth = 0;

		public int getPartition(IntWritable key, Text value, int numPartitions) {
			if (key.get() == 2001) {
				return 0;
			}
			if (key.get() == 2002) {
				return 1;
			}
			if (key.get() == 2003) {
				return 2;
			}
			if (key.get() == 2004)
				return 3;
			else
				return 4;
		}

		public Configuration getConf() {
			return conf;
		}

		public void setConf(Configuration conf) {
			this.conf = conf;
			minLastAccessDateMonth = conf.getInt(MIN_LAST_ACCESS_DATE_MONTH, 0);
		}

		public static void setMinLastAccessDate(Job job, int minLastAccessDateYear) {
			job.getConfiguration().setInt(MIN_LAST_ACCESS_DATE_MONTH, minLastAccessDateMonth);
		}
	}

	public static class Final_partition_date_reducer extends Reducer<IntWritable, Text, Text, NullWritable> {

		protected void reduce(IntWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			for (Text t : values) {
				context.write(t, NullWritable.get());
			}
		}

	}

}
