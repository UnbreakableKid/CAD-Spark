package cadlabs;

import static org.junit.Assert.assertEquals;

import org.apache.spark.sql.SparkSession;
import org.junit.Test;


public abstract class AbstractBaseTest<Input, Result> {

	protected SparkSession spark;
	
	
	protected void startSpark() {
		// start Spark session (SparkContext API may also be used) 
		// master("local") indicates local execution
		spark = SparkSession.builder().
				appName("FlightAnalyser").
				master("local").
				getOrCreate();
		
		
		// only error messages are logged from this point onward
		// comment (or change configuration) if you want the entire log
		spark.sparkContext().setLogLevel("ERROR");
	}


	/**
	 * Pre-processing of the input file to generate a dataset of type Input
	 *
	 * @param file The input file
	 * @return The generated dataset
	 */
	protected abstract Input processInputFile(String file);

	/**
	 * The test to execute over the input dataset (of type Input)
	 *
	 * @param dataset The input dataset
	 * @return The result of the test
	 */
	protected abstract Result run(Input dataset);

	/**
	 * The result the test should produce.
	 * If the method returns null, the output of the test is not tested.
	 * @return
	 */
	protected abstract String expectedResult();


	@Test
	public void run() {
		
		String file = "data/flights.csv";
		
		startSpark();

		Input flights = processInputFile(file);

		long start = System.currentTimeMillis();
		Result result = run(flights);
		long elapsed = System.currentTimeMillis() - start;
		
		System.err.println("Elapsed Time: " + elapsed);

		String expectedResult = expectedResult();
		if (expectedResult != null) {
			System.err.println("Asserting the correctness of the test's result");
			assertEquals(result.toString(), expectedResult());
		}
		// terminate the session
		spark.stop();
	}

}
