package cadlabs.rdd;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

import java.util.Map;

public class FlightsPerRoute extends AbstractFlightAnalyser<Map<String, Long>> {


	public FlightsPerRoute(JavaRDD<Flight> flights) {
		super(flights);
	}
	
	
	public Map<String, Long> run() {
		JavaPairRDD<String, String> allRoutes =
					this.flights.mapToPair(flight -> new Tuple2<>(flight.origin, flight.dest));
			
		return allRoutes.;
	}

}
