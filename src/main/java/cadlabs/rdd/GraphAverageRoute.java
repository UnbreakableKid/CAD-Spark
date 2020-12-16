package cadlabs.rdd;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.linalg.distributed.CoordinateMatrix;
import org.apache.spark.mllib.linalg.distributed.IndexedRowMatrix;
import org.apache.spark.mllib.linalg.distributed.MatrixEntry;
import scala.Tuple2;

public class GraphAverageRoute extends AbstractFlightAnalyser<IndexedRowMatrix> {


	public GraphAverageRoute(JavaRDD<Flight> flights) {
		super(flights);
	}
	
	public IndexedRowMatrix run(){
		JavaPairRDD<Tuple2<Long,Long>, Double> flightAverage =
				this.flights.mapToPair(
						flight ->
								new Tuple2<>(new Tuple2<>(flight.origInternalId, flight.destInternalId), new Tuple2<>(flight.arrtime - flight.deptime,1 ))
				)
						.reduceByKey(
								(d1, d2) -> new Tuple2<>(d1._1 + d2._1, d2._2 + d1._2 )
						)
				.mapToPair(flightz -> new Tuple2<>(flightz._1, (flightz._2._1 / flightz._2._2)));

		JavaRDD<MatrixEntry> entries = flightAverage.map(flight -> new MatrixEntry(flight._1._2, flight._1._1, flight._2 ));

		CoordinateMatrix test = new CoordinateMatrix(entries.rdd());
		return test.toIndexedRowMatrix();
	}
	
}
