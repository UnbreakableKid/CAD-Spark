package cadlabs.seq;

import cadlabs.rdd.AbstractTest;
import cadlabs.rdd.Flight;
import cadlabs.rdd.Path;
import org.apache.spark.api.java.JavaRDD;


public class SSSPTest extends AbstractTest<Path> {


	@Override
	protected Path run(JavaRDD<Flight> flights) {

		long time = System.currentTimeMillis();
		Path route = new SSSP("TPA", "PSG", flights).run();
		long elapsed =  System.currentTimeMillis() - time;
		System.out.println("Route " + route + "\nComputed in " + elapsed + " ms.");
		return route;
	}

	@Override
	protected String expectedResult() {
		return null;
	}	
}
