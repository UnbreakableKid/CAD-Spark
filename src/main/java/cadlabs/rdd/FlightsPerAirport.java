package cadlabs.rdd;

import java.util.Map;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import scala.Tuple2;

public class FlightsPerAirport extends AbstractFlightAnalyser<Map<String, Long>> {

	
	public FlightsPerAirport(JavaRDD<Flight> flights) {
		super(flights);
	}
	
	
	
	public Map<String, Long> run() {
		JavaPairRDD<String, String> allRoutes = 
					this.flights.mapToPair(flight -> new Tuple2<>(flight.origin, flight.dest));
			
		return allRoutes.countByKey();
	}

}
