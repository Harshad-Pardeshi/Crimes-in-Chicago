package project.chicagocrimes.frequency;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class FrequencyPartitioner extends Partitioner<CompositeKeyWritable, NullWritable>{

    @Override
    public int getPartition(CompositeKeyWritable key, NullWritable value, int numOfPartitions) {
        
        return (key.getLocation().hashCode() % numOfPartitions);
    }
}