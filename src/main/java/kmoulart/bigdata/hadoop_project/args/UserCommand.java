package kmoulart.bigdata.hadoop_project.args;


import java.io.IOException;

import org.apache.mahout.classifier.df.data.DescriptorException;

import com.beust.jcommander.JCommander;


/**
 * Processes a valid command.
 * 
 */
public interface UserCommand {

	/**
	 * Processes the command.
	 * @throws IOException 
	 * @throws ClassNotFoundException 
	 * @throws InterruptedException 
	 * @throws DescriptorException 
	 */
	public void process(final JCommander jCommander) throws IOException, InterruptedException, ClassNotFoundException, DescriptorException;
}
