package project.chicagocrimes.binning;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import project.chicagocrimes.partitioning.Partitioning.Final_partition_date_mapper;

public class BinningByMonth {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		try {
			Configuration conf = new Configuration();
			Job job = Job.getInstance(conf, "BinningByYear");
			job.setJarByClass(BinningByMonth.class);
			job.setMapperClass(MapperClass.class);
			MultipleOutputs.addNamedOutput(job, "bins", TextOutputFormat.class, Text.class, NullWritable.class);
			MultipleOutputs.setCountersEnabled(job, true);
			job.setNumReduceTasks(0);
			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			System.exit(job.waitForCompletion(true) ? 0 : 1);
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}

	}

	public static class MapperClass extends Mapper<Object, Text, Text, NullWritable> {

		private MultipleOutputs<Text, NullWritable> mos = null;

		protected void setup(Context context) {
			mos = new MultipleOutputs(context);
		}

		private static SimpleDateFormat frmt = new SimpleDateFormat("MM/dd/yyyy");
		Calendar cal = Calendar.getInstance();

		@Override
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			String[] separatedInput = value.toString().split("\t");

			String crimeString = separatedInput[1];
			if (crimeString == null) {
				return;
			}

			String copyOfCrime = new String();

			boolean inQuotes = false;

			for (int i = 0; i < crimeString.length(); ++i) {
				if (crimeString.charAt(i) == '"')
					inQuotes = !inQuotes;
				if (crimeString.charAt(i) == ',' && inQuotes)
					copyOfCrime += '|';
				else
					copyOfCrime += crimeString.charAt(i);
			}

			String[] separatedCrime = copyOfCrime.toString().split(",");
			String timeStamp = separatedCrime[3];
			if (timeStamp == null || timeStamp == "") {
				return;
			}

			
			if (timeStamp.toString().contains("Date")) /* Some condition satisfying it is header */
				return;
			else {
				String[] words = timeStamp.toString().split(" ");
				try {
					cal.setTime(frmt.parse(words[0].trim()));
				} catch (ParseException ex) {
					Logger.getLogger(Final_partition_date_mapper.class.getName()).log(Level.SEVERE, null, ex);
				}
			}
			
			String month = String.valueOf(cal.get(Calendar.MONTH));
			// The foreign join key is the user ID

			// int yearBin = Integer.parseInt(year);

			mos.write("bins", value, NullWritable.get(), month + "-bin");
			/*
			 * for(String s:str) { if("00".equals(s1)) {
			 * mos.write("bins",value,NullWritable.get(),"12am-bin"); }
			 * if("01".equals(s1)) {
			 * mos.write("bins",value,NullWritable.get(),"01am-bin"); }
			 * if("02".equals(s1)) {
			 * mos.write("bins",value,NullWritable.get(),"02am-bin"); }
			 * if("03".equals(s1)) {
			 * mos.write("bins",value,NullWritable.get(),"03am-bin"); }
			 * if("04".equals(s1)) {
			 * mos.write("bins",value,NullWritable.get(),"04am-bin"); }
			 * if("05".equals(s1)) {
			 * mos.write("bins",value,NullWritable.get(),"05am-bin"); }
			 * if("06".equals(s1)) {
			 * mos.write("bins",value,NullWritable.get(),"06am-bin"); }
			 * if("07".equals(s1)) {
			 * mos.write("bins",value,NullWritable.get(),"07am-bin"); }
			 * if("08".equals(s1)) {
			 * mos.write("bins",value,NullWritable.get(),"08am-bin"); }
			 * if("09".equals(s1)) {
			 * mos.write("bins",value,NullWritable.get(),"09am-bin"); }
			 * if("10".equals(s1)) {
			 * mos.write("bins",value,NullWritable.get(),"10am-bin"); }
			 * if("11".equals(s1)) {
			 * mos.write("bins",value,NullWritable.get(),"11am-bin"); }
			 * if("12".equals(s1)) {
			 * mos.write("bins",value,NullWritable.get(),"12pm-bin"); }
			 * if("13".equals(s1)) {
			 * mos.write("bins",value,NullWritable.get(),"01pm-bin"); }
			 * if("14".equals(s1)) {
			 * mos.write("bins",value,NullWritable.get(),"02pm-bin"); }
			 * if("15".equals(s1)) {
			 * mos.write("bins",value,NullWritable.get(),"03pm-bin"); }
			 * if("16".equals(s1)) {
			 * mos.write("bins",value,NullWritable.get(),"04pm-bin"); }
			 * if("17".equals(s1)) {
			 * mos.write("bins",value,NullWritable.get(),"05pm-bin"); }
			 * if("18".equals(s1)) {
			 * mos.write("bins",value,NullWritable.get(),"06pm-bin"); }
			 * if("19".equals(s1)) {
			 * mos.write("bins",value,NullWritable.get(),"07pm-bin"); }
			 * if("20".equals(s1)) {
			 * mos.write("bins",value,NullWritable.get(),"08pm-bin"); }
			 * if("21".equals(s1)) {
			 * mos.write("bins",value,NullWritable.get(),"09pm-bin"); }
			 * if("22".equals(s1)) {
			 * mos.write("bins",value,NullWritable.get(),"10pm-bin");
			 * }if("23".equals(s1)) {
			 * mos.write("bins",value,NullWritable.get(),"11pm-bin"); } }
			 */

		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			// super.cleanup(context); //To change body of generated methods,
			// choose Tools | Templates.
			mos.close();
		}

	}

}
