package project.chicagocrimes.frequency;
/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/**
 *
 * @author harshad
 */
public class FrequencyByDistrict {

	public static class FrequencyByDistrictMapper extends Mapper<Object, Text, Text, LongWritable> {

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
			String districtId = separatedCrime[13];
			if (districtId == null) {
				return;
			}
			// The foreign join key is the user ID
			Text outKey = new Text(districtId);
			context.write(outKey, one);
		}
	}

	public static 	class FrequencyByDistrictReducer extends Reducer<Text, LongWritable, Text, LongWritable>{

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

	public static class FrequencyMapper2 extends Mapper<LongWritable, Text, CompositeKeyWritable, IntWritable>{

	    @Override
	    protected void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException {
	        Text districtId = new Text();
	        LongWritable count = new LongWritable();
	        if(values.toString().length()>0)
	        {
	            try{
	                String str[] = values.toString().split("\t");
	                districtId.set(str[0]);
	                count.set(Long.parseLong(str[1]));      
	   
	                CompositeKeyWritable cw = new CompositeKeyWritable(districtId.toString(), count.get());
	                context.write(cw, new IntWritable(1));
	           }catch(IOException | InterruptedException ex){
	                Logger.getLogger(FrequencyMapper2.class.getName()).log(Level.SEVERE, null, ex);
	           }
	        }
	    }
	}

	public static class FrequencyReducer2 extends Reducer<CompositeKeyWritable, IntWritable, CompositeKeyWritable, IntWritable> {

	    public static int count = 0;

	    @Override
	    protected void reduce(CompositeKeyWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
	        for (IntWritable val : values) {

	            if (count < 25) {
	                context.write(key, new IntWritable(count));
	                count++;
	            } else {
	                break;
	            }
	        }
	    }
	}
	

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "FrequencyByDistrict");
        job.setJarByClass(FrequencyByDistrict.class);
        job.setMapperClass(FrequencyByDistrictMapper.class);

//        job.setCombinerClass(MovieReducer1.class);
        job.setReducerClass(FrequencyByDistrictReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean complete = job.waitForCompletion(true);
//        System.exit(job.waitForCompletion(true)? 0:1);
		
        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2, "Chaining");

        if (complete) {
            job2.setJarByClass(FrequencyByDistrict.class);
            job2.setMapperClass(FrequencyMapper2.class);
//            job2.setCombinerClass(MovieReducer2.class);
            job2.setReducerClass(FrequencyReducer2.class);
            job2.setOutputKeyClass(CompositeKeyWritable.class);
            job2.setOutputValueClass(IntWritable.class);

            FileInputFormat.addInputPath(job2, new Path(args[1]));
            FileOutputFormat.setOutputPath(job2, new Path(args[2]));
            System.exit(job2.waitForCompletion(true) ? 0 : 1);
        }
		
	}

}
