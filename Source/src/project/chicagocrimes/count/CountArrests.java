package project.chicagocrimes.count;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CountArrests {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        // TODO code application logic here
        Path input = new Path(args[0]);
        Path outputDir = new Path(args[1]);

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "count");
        job.setJarByClass(CountArrests.class);

        job.setMapperClass(CountMapper.class);
        job.setNumReduceTasks(0);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(NullWritable.class);

        //job.setInputFormatClass(FileInputFormat.class);
        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, outputDir);

        int code = job.waitForCompletion(true) ? 0 : 1;

        if (code == 0) {
            for (Counter counter : job.getCounters().getGroup(CountMapper.ARREST_TYPE)) {
                System.out.println(counter.getDisplayName() + "\t" + counter.getValue());
            }
        }

        FileSystem.get(conf).delete(outputDir, true);
        
        System.exit(code);
        

    }

}
