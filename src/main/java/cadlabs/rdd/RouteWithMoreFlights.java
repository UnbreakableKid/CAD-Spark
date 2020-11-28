package cadlabs.rdd;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

import java.util.List;
import java.util.Map;

public class RouteWithMoreFlights extends AbstractFlightAnalyser<Tuple2<Tuple2<String, String>, Long>> {


	public RouteWithMoreFlights(JavaRDD<Flight> flights) {
		super(flights);
	}
	
	
	public Tuple2<Tuple2<String, String>, Long> run() {
		JavaPairRDD<Tuple2<String, String>, Long> allRoutes =
				this.flights.mapToPair(flight -> new Tuple2<>(new Tuple2<>(flight.origin, flight.dest) , (long)1));

		JavaPairRDD<Tuple2<String, String>, Long> test = allRoutes.reduceByKey(Long::sum);


		JavaPairRDD<Tuple2<String, String>, Long> sorted =  // sort by value
				test.mapToPair(Tuple2::swap).sortByKey(false).
						mapToPair(Tuple2::swap);

		List<Tuple2<Tuple2<String, String>, Long>> fixed = sorted.take(1);
		return fixed.get(0);
	}

}
