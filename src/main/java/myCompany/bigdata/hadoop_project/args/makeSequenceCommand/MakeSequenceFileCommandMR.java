package myCompany.bigdata.hadoop_project.args.makeSequenceCommand;

import java.io.IOException;

import myCompany.bigdata.hadoop_project.args.UserCommand;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.mahout.math.VectorWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

/**
 * Transforms, using a MapReduce Job, the given CSV into a Sequence File Text/VectorWritable with
 * \"key/key\" as key and the features as vector.
 * 
 */
@Parameters(commandDescription = "Transforms, using a MapReduce Job, the given CSV into a Sequence File Text/VectorWritable with \"key/key\" as key and the features as vector.")
public class MakeSequenceFileCommandMR implements UserCommand {


	private static final Logger LOGGER = LoggerFactory.getLogger(MakeSequenceFileCommandMR.class);
	
	/**
	 * The CSV file to tranform (in HDFS).
	 */
	@Parameter(names = {"-i", "--input"}, description = "The CSV file to transform (in HDFS).", required = true)
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
	public void process(final JCommander jCommander) throws IOException, InterruptedException, ClassNotFoundException {
		Path inputPath = new Path(input);
		Path outputDir = new Path(output);

		// Create configuration
		Configuration conf = new Configuration(true);
		conf.set("separator", separator);

		// Create job
		Job job = new Job(conf, "ToSequenceFile");
		job.setJarByClass(MakeSequenceFileMapper.class);

		// Setup MapReduce
		job.setMapperClass(MakeSequenceFileMapper.class);
		job.setNumReduceTasks(0);

		// Specify key / value
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(VectorWritable.class);

		// Input
		FileInputFormat.addInputPath(job, inputPath);
		job.setInputFormatClass(TextInputFormat.class);

		// Output
		FileOutputFormat.setOutputPath(job, outputDir);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		// Delete output if exists
		FileSystem hdfs = FileSystem.get(conf);
		if (hdfs.exists(outputDir)) {
			if (overwrite) {
				hdfs.delete(outputDir, true);
			} else {
				LOGGER.error("The ouput file already exists");
				return;
			}
		}


		// Execute job
		int code = job.waitForCompletion(true) ? 0 : 1;
		System.exit(code);
	}

}
