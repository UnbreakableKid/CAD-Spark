package cadlabs.rdd;

import org.apache.spark.api.java.JavaRDD;

public abstract class AbstractFlightAnalyser<T> {

	protected final JavaRDD<Flight> flights;
	
	public AbstractFlightAnalyser(JavaRDD<Flight> flights) {
		this.flights = flights;
	}
	
	public abstract T run();
	
}
