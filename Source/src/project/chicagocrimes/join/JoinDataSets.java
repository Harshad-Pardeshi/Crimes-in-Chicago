package project.chicagocrimes.join;
/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


/**
 *
 * @author harshad
 */
public class JoinDataSets {

	public static class CrimeMapper extends Mapper<Object, Text, Text, Text> {

		private Text outkey = new Text();
		private Text outvalue = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			// Parse the input string into a nice map
			String[] separatedInput = value.toString().split(",");
			String iucrnNumber = separatedInput[5];
//			String bookName = separatedInput[1];
			if (iucrnNumber == null) {
				return;
			}
			// The foreign join key is the user ID
			outkey.set(iucrnNumber);
			// Flag this record for the reducer and then output
			outvalue.set("A|" + value);
			context.write(outkey, outvalue);
		}
	}

	public static class IUCRMapper extends Mapper<Object, Text, Text, Text> {

		private Text outkey = new Text();
		private Text outvalue = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] separatedInput = value.toString().split(",");

			String iucrNumber = separatedInput[0];
//			String rating = separatedInput[2];
			if (iucrNumber == null) {
				return;
			}
			// The foreign join key is the user ID
			outkey.set(iucrNumber);
			// Flag this record for the reducer and then output
			outvalue.set("B|" + value);
			context.write(outkey, outvalue);
		}
	}

	public static class DistrictMapper extends Mapper<Object, Text, Text, Text> {

		private Text outkey = new Text();
		private Text outvalue = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] separatedInput = value.toString().split(",");
			String districtId = separatedInput[0];
//			String rating = separatedInput[2];
			if (districtId == null) {
				return;
			}
			// The foreign join key is the user ID
			outkey.set(districtId);
			// Flag this record for the reducer and then output
			outvalue.set("A|" + value);
			context.write(outkey, outvalue);
		}
	}
	
	public static class OutputOfFirstMapper extends Mapper<Object, Text, Text, Text> {

		private Text outkey = new Text();
		private Text outvalue = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] separatedInput = value.toString().split("\t");

			String crimeString = separatedInput[0];
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
			String districtId = separatedCrime[12];
			if (districtId == null) {
				return;
			}
			// The foreign join key is the user ID
			outkey.set(districtId);
			// Flag this record for the reducer and then output
			outvalue.set("B|" + value);
			context.write(outkey, outvalue);
		}
	}

	public static class JoinReducer extends Reducer<Text, Text, Text, Text> {

		private static final Text EMPTY_TEXT = new Text("");
		private Text tmp = new Text();
		private ArrayList<Text> listA = new ArrayList<Text>();
		private ArrayList<Text> listB = new ArrayList<Text>();
		private String joinType = null;

		public void setup(Context context) {
			// Get the type of join from our configuration
			joinType = context.getConfiguration().get("join.type");
		}

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			// Clear our lists
			listA.clear();
			listB.clear();
			// iterate through all our values, binning each record based on what
			// it was tagged with. Make sure to remove the tag!
			while (values.iterator().hasNext()) {
				tmp = values.iterator().next();
				System.out.println(Character.toString((char) tmp.charAt(0)));
				if ((Character.toString((char) tmp.charAt(0)).equals("A"))
						&& (Character.toString((char) tmp.charAt(1)).equals("|"))) {
					listA.add(new Text(tmp.toString().substring(2)));
				}
				if ((Character.toString((char) tmp.charAt(0)).equals("B"))
						&& (Character.toString((char) tmp.charAt(1)).equals("|"))) {
					listB.add(new Text(tmp.toString().substring(2)));
				}
			}
			// Execute our join logic now that the lists are filled
			System.out.println(listB.size());
			executeJoinLogic(context);
		}

		private void executeJoinLogic(Context context) throws IOException, InterruptedException {

			if (joinType.equalsIgnoreCase("inner")) {
				// If both lists are not empty, join A with B
				// System.out.println("here3");
				if (!listA.isEmpty() && !listB.isEmpty()) {
					System.out.println("here");
					for (Text A : listA) {
						// System.out.println("here1");
						for (Text B : listB) {
							// System.out.println("here2");
							context.write(A, B);
						}
					}
				}
			} else if (joinType.equalsIgnoreCase("leftouter")) {
				// For each entry in A,
				for (Text A : listA) {
					// If list B is not empty, join A and B
					if (!listB.isEmpty()) {
						for (Text B : listB) {
							context.write(A, B);
						}
					} else {
						// Else, output A by itself
						context.write(A, EMPTY_TEXT);
					}
				}
			} else if (joinType.equalsIgnoreCase("rightouter")) {
				// For each entry in B,
				for (Text B : listB) {
					// If list A is not empty, join A and B
					if (!listA.isEmpty()) {
						for (Text A : listA) {
							context.write(A, B);
						}
					} else {
						// Else, output B by itself
						context.write(EMPTY_TEXT, B);
					}
				}
			} else if (joinType.equalsIgnoreCase("fullouter")) {
				// If list A is not empty
				if (!listA.isEmpty()) {
					// For each entry in A
					for (Text A : listA) {
						// If list B is not empty, join A with B
						if (!listB.isEmpty()) {
							for (Text B : listB) {
								context.write(A, B);
							}
						} else {
							// Else, output A by itself
							context.write(A, EMPTY_TEXT);
						}
					}
				} else {
					// If list A is empty, just output B
					for (Text B : listB) {
						context.write(EMPTY_TEXT, B);
					}
				}
			} else if (joinType.equalsIgnoreCase("anti")) {
				// If list A is empty and B is empty or vice versa
				if (listA.isEmpty() ^ listB.isEmpty()) {
					// Iterate both A and B with null values
					// The previous XOR check will make sure exactly one of
					// these lists is empty and therefore the list will be
					// skipped
					for (Text A : listA) {
						context.write(A, EMPTY_TEXT);
					}
					for (Text B : listB) {
						context.write(EMPTY_TEXT, B);
					}
				}
			}
		}
	}
	
	

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf, "ReduceSideJoin");
		job.setJarByClass(JoinDataSets.class);

		// Use MultipleInputs to set which input uses what mapper
		// This will keep parsing of each data set separate from a logical
		// standpoint
		// The first two elements of the args array are the two inputs
		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, CrimeMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, IUCRMapper.class);
		job.getConfiguration().set("join.type", "leftouter");
		// job.setNumReduceTasks(0);
		job.setReducerClass(JoinReducer.class);

		job.setOutputFormatClass(TextOutputFormat.class);
		TextOutputFormat.setOutputPath(job, new Path(args[3]));

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		boolean complete = job.waitForCompletion(true);

//		System.exit(job.waitForCompletion(true) ? 0 : 1);
		Configuration conf2 = new Configuration();
		Job job2 = Job.getInstance(conf2, "ReduceSideJoinChaining");

		if (complete) {
			job2.setJarByClass(JoinDataSets.class);

			MultipleInputs.addInputPath(job2, new Path(args[2]), TextInputFormat.class, DistrictMapper.class);
			MultipleInputs.addInputPath(job2, new Path(args[3]), TextInputFormat.class, OutputOfFirstMapper.class);
			job2.getConfiguration().set("join.type", "rightouter");
			// job.setNumReduceTasks(0);
			job2.setReducerClass(JoinReducer.class);

			job2.setOutputFormatClass(TextOutputFormat.class);
			TextOutputFormat.setOutputPath(job2, new Path(args[4]));

			job2.setOutputKeyClass(Text.class);
			job2.setOutputValueClass(Text.class);

			complete = job2.waitForCompletion(true);
			 
			System.exit(job2.waitForCompletion(true) ? 0 : 1);
		}

	}

}
