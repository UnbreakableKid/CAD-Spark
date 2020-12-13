package cadlabs.rdd;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.linalg.distributed.IndexedRow;
import org.apache.spark.mllib.linalg.distributed.IndexedRowMatrix;

public class GraphRouteAverage extends AbstractTest<IndexedRowMatrix> {

	@Override
	protected IndexedRowMatrix run(JavaRDD<Flight> flights) {

		IndexedRowMatrix test = new GraphAverageRoute(flights).run();
		for (IndexedRow e: test.rows().toJavaRDD().collect()
			 ) {
			System.out.println(e);
			
		}

		return test;
	}

	@Override
	protected String expectedResult() {
		return "hi";
	}
}

