package cadlabs.rdd;


import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

public class RouteWithMoreFlightsTest extends AbstractTest<Tuple2<Tuple2<String, String>, Long>> {

	@Override
	protected Tuple2<Tuple2<String, String>, Long> run(JavaRDD<Flight> flights) {
		return null; // new RouteWithMoreFlights(flights).run();
	}

	@Override
	protected String expectedResult() {
		return "((SFO,LAX),1080)";
	}	
}
