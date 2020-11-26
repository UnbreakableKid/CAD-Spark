package cadlabs.rdd;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

import java.util.Map;

public class RouteWithMoreFlights extends AbstractFlightAnalyser<Map<Tuple2<String, String>, Long>> {


	public RouteWithMoreFlights(JavaRDD<Flight> flights) {
		super(flights);
	}
	
	
	public Map<Tuple2<String, String>, Long> run() {
		JavaPairRDD<Tuple2<String, String>, Long> allRoutes =
				this.flights.mapToPair(flight -> new Tuple2<>(new Tuple2<>(flight.origin, flight.dest) , (long)1));


		JavaPairRDD<Tuple2<String, String>, Long> sorted =  // sort by value
				allRoutes.mapToPair(x -> x.swap()).sortByKey(false).
						mapToPair(x -> x.swap());
		return sorted.countByKey();
	}

}
