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
	
	
	protected abstract Input processInputFile(String file);
	
	protected abstract Result run(Input flights);
	
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
		assertEquals(result.toString(), expectedResult());
		
		// terminate the session
		spark.stop();
	}

}
