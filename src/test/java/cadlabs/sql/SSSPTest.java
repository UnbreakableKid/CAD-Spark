package cadlabs.sql;

import cadlabs.rdd.AbstractTest;
import cadlabs.rdd.Flight;
import cadlabs.rdd.Path;
import org.apache.spark.api.java.JavaRDD;


public class SSSPTest extends AbstractTest<Path> {


	@Override
	protected Path run(JavaRDD<Flight> flights) {

		long time = System.currentTimeMillis();
		Path route = new SSSPSpark("TPA", "PSG", flights, null).run();
		long elapsed =  System.currentTimeMillis() - time;
		System.out.println("Route " + route + " with weight " + route.getWeight() + "\nComputed in " + elapsed + " ms.");
		return route;
	}

	@Override
	protected String expectedResult() {
		return null;
	}	
}
