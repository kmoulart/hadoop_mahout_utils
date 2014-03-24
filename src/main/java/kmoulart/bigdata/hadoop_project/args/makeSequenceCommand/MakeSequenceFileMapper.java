package kmoulart.bigdata.hadoop_project.args.makeSequenceCommand;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
 
public class MakeSequenceFileMapper extends
        Mapper<Object, Text, Text, VectorWritable> {
  
    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
 
    	try {
			// Split with the given separator
			String[] c = value.toString().split(context.getConfiguration().get("separator", ","));
			if (c.length > 1) {
				double[] d = new double[c.length];
				// Get the feature set
				for (int i = 1; i < c.length; i++)
					d[i] = Double.parseDouble(c[i]);
				// Put it in a vector
				Vector vec = new RandomAccessSparseVector(c.length);
				vec.assign(d);

				VectorWritable writable = new VectorWritable();
				writable.set(vec);

				// Create a label with a / and the class label
				String label = c[0] + "/" + c[0];

				// Write all in the seqfile
				context.write(new Text(label), writable);
			}
		} catch (NumberFormatException e) {
		}
    }
}