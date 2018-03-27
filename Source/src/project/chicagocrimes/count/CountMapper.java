package project.chicagocrimes.count;

import java.util.Arrays;
import java.util.HashSet;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CountMapper extends Mapper<Object, Text, NullWritable, NullWritable> {

    public static final String ARREST_TYPE = "Arrest ";
    public static final String UNKNOWN_COUNTER = "Unknown";
    public static final String OTHER_COUNTER = "Other";
    public static final String NULL_OR_EMPTY = "Null or Empty";

    private String[] arrestArray = new String[]{"TRUE", "FALSE","True","False"};

    private HashSet<String> arrests = new HashSet<String>(Arrays.asList(arrestArray));

    public void map(Object key, Text value, Context context) {
    	
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
		String arrested = separatedCrime[9];
		String domestic = separatedCrime[10];
		if (arrested == null) {
			return;
		}


       
        boolean unknown = true;

        if (arrests.contains(arrested) && domestic.equalsIgnoreCase("True")) {
        	context.getCounter(ARREST_TYPE, arrested).increment(1);
            unknown = false;
        }

        if (unknown) {
            context.getCounter(ARREST_TYPE, UNKNOWN_COUNTER).increment(1);
        } /*else {
            context.getCounter(ARREST_TYPE, NULL_OR_EMPTY);

        }*/

    }
}
