package cadlabs.sql;

import cadlabs.AbstractSSSP;
import cadlabs.rdd.Flight;
import cadlabs.rdd.Path;
import org.apache.spark.api.java.JavaRDD;

public class SSSPMainSpark extends AbstractSSSP {

    /**
     * Source node
     */
    private final String source;

    /**
     * Destination node
     */
    private final String destination;


    /**
     * Constructor
     * @param URL URL of the Spark master
     * @param numberNodes Number of nodes in the graph
     * @param percentageOfConnections To how many nodes (in percentage) a node is connected to.
     * @param source Source node
     * @param destination Destination node
     */
    public SSSPMainSpark(String URL, int numberNodes, int percentageOfConnections, String source, String destination) {
        super(URL, numberNodes, percentageOfConnections);
        this.source = source;
        this.destination = destination;
    }


    @Override
    protected Path run(JavaRDD<Flight> flights) {
//        CoordinateMatrix cm = buildGraph(flights);
//        JavaPairRDD<Integer, Integer> x = new calculateD(flights, cm, Flight.getAirportIdFromName(this.source)).run();
        return new SSSPSpark(this.source, this.destination, flights).run();
    }


    public static void main(String[] args) {
        if(args[0].compareTo("info")==0)
        new SSSPMainSpark("local", 10, 70, "Node1",
                "Node9").run();
    }
}