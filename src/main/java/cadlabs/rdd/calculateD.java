package cadlabs.rdd;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.linalg.distributed.CoordinateMatrix;
import org.apache.spark.mllib.linalg.distributed.MatrixEntry;
import scala.Tuple2;

public class calculateD extends AbstractFlightAnalyser<JavaPairRDD<Integer, Integer>> {


    public calculateD(JavaRDD<Flight> flights) {
        super(flights);
    }

    private double getWeight(int origin, int dest, CoordinateMatrix graph) {
        for (MatrixEntry e : graph.entries().toJavaRDD().collect()) {
            if (e.i() == origin && e.j() == dest)
                return e.value();
        }
        return Double.MAX_VALUE;
    }


    public JavaPairRDD<Integer, Double> run(CoordinateMatrix graph, int source) {
        return
                this.flights.mapToPair(flight -> new Tuple2<>((int )flight.destInternalId,  getWeight(source, (int) flight.destInternalId, graph)));

    }

    @Override
    public JavaPairRDD<Integer, Integer> run() {
        return null;
    }
}
