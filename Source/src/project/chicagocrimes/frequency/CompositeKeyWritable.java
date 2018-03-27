package project.chicagocrimes.frequency;



import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

/**
 *
 * @author harshad
 */
public class CompositeKeyWritable implements Writable, WritableComparable<CompositeKeyWritable>{

    private String location;
    private Long totalCount;

    
    public CompositeKeyWritable()
    {
      
    }
   
    public CompositeKeyWritable(String l, Long c)
    {
        this.location = l;
        this.totalCount = c;
    }

        
    public String getLocation() {
		return location;
	}

	public void setLocation(String location) {
		this.location = location;
	}

	public Long getTotalCount() {
		return totalCount;
	}

	public void setTotalCount(Long totalCount) {
		this.totalCount = totalCount;
	}

	@Override
    public void write(DataOutput d) throws IOException {
		WritableUtils.writeString(d, location);
        d.writeLong(totalCount);
    }

    @Override
    public void readFields(DataInput di) throws IOException {
        location = WritableUtils.readString(di);
        totalCount = di.readLong();
    }

    @Override
    public int compareTo(CompositeKeyWritable o) {
/*    	int result = totalCount.compareTo(o.totalCount);
        if(result ==0)
        {
            result = location.compareTo(o.location);
        }
        return result;
*/    	
       return -1*(totalCount.compareTo(o.totalCount));
    }

    @Override
    public String toString() {
        return location + "\t" + totalCount;
    }
}
