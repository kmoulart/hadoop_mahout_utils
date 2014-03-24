package kmoulart.bigdata.hadoop_project.args.makeSequenceCommand;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import kmoulart.bigdata.hadoop_project.args.UserCommand;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

/**
 * Sequentially transforms the given CSV into a Sequence File Text/VectorWritable with
 * \"key/key\" as key and the features as vector.
 * 
 */
@Parameters(commandDescription = "Sequentially transforms the given CSV into a Sequence File Text/VectorWritable with \"key/key\" as key and the features as vector.")
public class MakeSequenceFileCommand implements UserCommand {


	private static final Logger LOGGER = LoggerFactory.getLogger(MakeSequenceFileCommand.class);
	
	/**
	 * The CSV file to tranform (in local file system).
	 */
	@Parameter(names = {"-i", "--input"}, description = "The CSV file to transform (in local file system).", required = true)
	private String input = null;
	
	/**
	 * The file into which the result shall be written (in HDFS).
	 */
	@Parameter(names = {"-o", "--output"}, description = "The file into which the result shall be written (in HDFS).", required = true)
	private String output = null;
		
	/**
	 * The separator used to separate the fields.
	 */
	@Parameter(names = {"-s", "--separator"}, description = "The separator used to separate the fields.", required = false)
	private String separator = ",";
	
	/**
	 * If set, the output file will be overwritten.
	 */
	@Parameter(names = {"-ow", "--overwrite"}, description = "If set, the output file will be overwritten.", required = false)
	private boolean overwrite = false;
	
	@SuppressWarnings("deprecation")
	public void process(final JCommander jCommander) throws IOException {
		Configuration conf = new Configuration(true);
		FileSystem fs = FileSystem.get(conf);

		Path filePath = new Path(output);

		// Delete previous file if exists
		if (fs.exists(filePath)) {
			if (overwrite) {
				fs.delete(filePath, true);
			} else {
				LOGGER.error("The ouput file already exists");
				return;
			}
		}

		
		// The input file is not in hdfs
		BufferedReader reader = new BufferedReader(new FileReader(input));
		
		SequenceFile.Writer writer = new SequenceFile.Writer(fs, conf,
				filePath, Text.class, VectorWritable.class);

		// Run through the input file
		String line;
		while ((line = reader.readLine()) != null) {
			// We surround with try catch to get rid of the exception when
			// header is included in file
			try {
				// Split with the given separator
				String[] c = line.split(separator);
				if (c.length > 1) {
					double[] d = new double[c.length];
					// Get the feature setl
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
					writer.append(new Text(label), writable);
				}
			} catch (NumberFormatException e) {
				continue;
			}
		}
		writer.close();

		reader.close();
	}

}
