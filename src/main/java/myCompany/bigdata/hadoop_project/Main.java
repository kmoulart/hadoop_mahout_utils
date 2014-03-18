package myCompany.bigdata.hadoop_project;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import myCompany.bigdata.hadoop_project.args.HelpCommand;
import myCompany.bigdata.hadoop_project.args.UserCommand;
import myCompany.bigdata.hadoop_project.args.RandomForest.MahoutRandomForestCommand;
import myCompany.bigdata.hadoop_project.args.makeSequenceCommand.MakeSequenceFileCommand;
import myCompany.bigdata.hadoop_project.args.makeSequenceCommand.MakeSequenceFileCommandMR;

import org.apache.mahout.classifier.df.data.DescriptorException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;

public class Main {

	private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);

	/**
	 * Reads the arguments, then executes the user request.
	 * 
	 * @param args
	 *            The arguments.
	 */
	public static void main(final String args[]) {

		final JCommander jCommander = new JCommander();
		final Map<String, UserCommand> commands = new HashMap<String, UserCommand>();

		initCommands(jCommander, commands);

		try {
			// First, we parse the arguments
			jCommander.parse(args);

			final String commandName = jCommander.getParsedCommand();

			// Then, we execute the user request.
			if (commandName == null) {
				jCommander.usage();
			} else {
				commands.get(commandName).process(jCommander);
			}
		} catch (ParameterException e) {
			jCommander.usage();
			LOGGER.error(e.getMessage());
		} catch (IOException e) {
			System.err.println("One or multiple file path doesn't exist");
			LOGGER.error(e.getMessage());
		} catch (InterruptedException e) {
			System.err.println("The job got interrupted");
			LOGGER.error(e.getMessage());
		} catch (ClassNotFoundException e) {
			System.err.println("Class not found");
			LOGGER.error(e.getMessage());
		} catch (DescriptorException e) {
			System.err.println("Wrong descriptor format");
			LOGGER.error(e.getMessage());
		}
	}

	/**
	 * Initializes JCommander and a UserCommand map.
	 * 
	 * The map will contain couples of (command name, command).
	 * 
	 * @param jCommander
	 *            The JCommander to initialize.
	 * @param commands
	 *            The map to fill with the existing commands.
	 */
	private static void initCommands(final JCommander jCommander, final Map<String, UserCommand> commands) {
		// The possible commands
		final HelpCommand helpCommand = new HelpCommand();
		final MakeSequenceFileCommand makeSequenceFileCommand = new MakeSequenceFileCommand();
		final MakeSequenceFileCommandMR makeSequenceFileCommandMR = new MakeSequenceFileCommandMR();
		final MahoutRandomForestCommand mahoutRandomForestCommand = new MahoutRandomForestCommand();

		// The help command will use the keyword "help"
		commands.put("help", helpCommand);

		// The MakeSequenceFileCommand with its keyword makesf
		commands.put("makesf", makeSequenceFileCommand);

		// The MakeSequenceFileCommandMR with its keyword makesfmr
		commands.put("makesfmr", makeSequenceFileCommandMR);

		// The mahoutRandomForestCommand with its keyword randforest
		commands.put("randforest", mahoutRandomForestCommand);
		
		

		for (Entry<String, UserCommand> commandEntry : commands.entrySet()) {
			jCommander.addCommand(commandEntry.getKey(), commandEntry.getValue());
		}
	}
}
