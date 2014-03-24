package kmoulart.bigdata.hadoop_project.args.RandomForest;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Random;

import kmoulart.bigdata.hadoop_project.args.UserCommand;

import org.apache.mahout.classifier.df.DecisionForest;
import org.apache.mahout.classifier.df.builder.DefaultTreeBuilder;
import org.apache.mahout.classifier.df.data.Data;
import org.apache.mahout.classifier.df.data.DataLoader;
import org.apache.mahout.classifier.df.data.Dataset;
import org.apache.mahout.classifier.df.data.DescriptorException;
import org.apache.mahout.classifier.df.data.DescriptorUtils;
import org.apache.mahout.classifier.df.data.Instance;
import org.apache.mahout.classifier.df.ref.SequentialBuilder;
import org.apache.mahout.common.RandomUtils;
import org.uncommons.maths.Maths;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

/**
 * Builds a random forest classifier on the provided training data and then test
 * it on the provided testing data.
 * 
 */
@Parameters(commandDescription = "Builds a random forest classifier on the provided training data and then test it on the provided testing data.")
public class MahoutRandomForestCommand implements UserCommand {


	/**
	 * The CSV training file to use to train the random forest classifier.
	 */
	@Parameter(names = { "-train", "--trainingFile" }, description = "The CSV training file to use to train the random forest classifier.", required = true)
	private String train = null;

	/**
	 * The CSV testing file to use to test the random forest classifier.
	 */
	@Parameter(names = { "-test", "--testFiles" }, description = "The folder containing the CSV testing file(s) to use to test the random forest classifier.", required = true)
	private String test = null;

	/**
	 * The number of trees to span.
	 */
	@Parameter(names = { "-n", "--numberOfTrees" }, description = "The number of trees to span.", required = false)
	private int numberOfTrees = 40;

	/**
	 * The types of data in the file (S = String, N = numeric, L = Label) in format like : 3 N 2 S L 4 N... .
	 */
	@Parameter(names = { "-types", "--typesDescriptor" }, description = "The types of data in the file (S = String, N = numeric, L = Label) in format like : 3 N 2 S L 4 N... .", required = true)
	private String descriptor = null;

	public void process(final JCommander jCommander) throws DescriptorException, IOException {

		String[] trainDataValues = fileAsStringArray(train);

		String validDescriptor = DescriptorUtils.generateDescriptor(descriptor);
		
		Data data = DataLoader.loadData(
				DataLoader.generateDataset(validDescriptor, false, trainDataValues),
				trainDataValues);
		// frees the heap
		trainDataValues = null;
		
		DecisionForest forest = buildForest(numberOfTrees, data);
		
		// frees the heap
		Dataset dataset = data.getDataset();
		data = null;
		
//		boolean endReached = false;
//		int nbRecordDone = 0;
//		while (!endReached) {
			String[] testDataValues = testFileAsStringArray(test);

			Data test = DataLoader.loadData(dataset, testDataValues);
			
			// frees the heap
			testDataValues = null;
			
			Random rng = RandomUtils.getRandom();

			for (int i = 0; i < test.size(); i++) {
				Instance oneSample = test.get(i);

				double classify = forest
						.classify(test.getDataset(), rng, oneSample);
				int label = dataset.valueOf(0,
						String.valueOf((int) classify));

				System.out.println("Label " + i + " : " + label);
			}
//		}
		
	}

	private static String[] testFileAsStringArray(String file) throws IOException {
		ArrayList<String> list = new ArrayList<String>();

		DataInputStream in = new DataInputStream(new FileInputStream(file));
		BufferedReader br = new BufferedReader(new InputStreamReader(in));

		String strLine;
		br.readLine(); // discard top one (header)
		while ((strLine = br.readLine()) != null) {
			// The substring is here to remove the label in front
			list.add("-," + strLine.substring(2));
		}

		in.close();
		return list.toArray(new String[list.size()]);
	}

	private static DecisionForest buildForest(int numberOfTrees, Data data) {
		int m = (int) Math
				.floor(Maths.log(2, data.getDataset().nbAttributes()) + 1);

		DefaultTreeBuilder treeBuilder = new DefaultTreeBuilder();
		treeBuilder.setM(m);

		return new SequentialBuilder(RandomUtils.getRandom(), treeBuilder,
				data.clone()).build(numberOfTrees);
	}

	private static String[] fileAsStringArray(String file) throws IOException {
		ArrayList<String> list = new ArrayList<String>();

		DataInputStream in = new DataInputStream(new FileInputStream(file));
		BufferedReader br = new BufferedReader(new InputStreamReader(in));

		String strLine;
		br.readLine(); // discard top one (header)
		while ((strLine = br.readLine()) != null) {
			list.add(strLine);
		}

		in.close();
		return list.toArray(new String[list.size()]);
	}
}
