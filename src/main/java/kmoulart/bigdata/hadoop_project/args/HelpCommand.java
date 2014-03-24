package kmoulart.bigdata.hadoop_project.args;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

/**
 * Represents an help command.
 * 
 */
@Parameters(commandDescription = "Displays the help about a specific command or about the software.")
public class HelpCommand implements UserCommand {

	/**
	 * The command for which the help is requested. If this argument is not specified, a generic help should be displayed.
	 */
	@Parameter(names = "-command", description = "The command for which the help is requested.")
	private String commandName = null;

	/**
	 * Returns the command name.
	 * 
	 * @return The command name.
	 */
	public String getCommandName() {
		return commandName;
	}

	/**
	 * Processes an help command. An helpful message will be displayed to the user.
	 */
	public void process(final JCommander jCommander) {
		if (commandName == null) {
			jCommander.usage();
		} else {
			jCommander.usage(commandName);
		}
	}
}