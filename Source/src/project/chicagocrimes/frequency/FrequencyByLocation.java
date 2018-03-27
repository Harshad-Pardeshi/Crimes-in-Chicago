package project.chicagocrimes.frequency;
/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/**
 *
 * @author harshad
 */
public class FrequencyByLocation {

	public static class FrequencyByLocationMapper extends Mapper<Object, Text, Text, LongWritable> {

		LongWritable one = new LongWritable(1);
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
			String districtId = separatedCrime[8];
			if (districtId == null) {
				return;
			}
			// The foreign join key is the user ID
			Text outKey = new Text(districtId);
			context.write(outKey, one);
		}
	}

	public static 	class FrequencyByLocationReducer extends Reducer<Text, LongWritable, Text, LongWritable>{

	    @Override
	    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
	        LongWritable result = new LongWritable();
	        long count = 0;
	        for(LongWritable val :values){
	            count++;
	        }
	        result.set(count);
	        context.write(key,result);
	    }
	}


	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "FrequencyByLocation");
        job.setJarByClass(FrequencyByLocation.class);
        job.setMapperClass(FrequencyByLocationMapper.class);

//        job.setCombinerClass(MovieReducer1.class);
        job.setReducerClass(FrequencyByLocationReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

//        boolean complete = job.waitForCompletion(true);
        System.exit(job.waitForCompletion(true)? 0:1);
		
		
	}

}
