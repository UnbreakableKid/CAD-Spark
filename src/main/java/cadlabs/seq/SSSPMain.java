package cadlabs.seq;

import cadlabs.AbstractSSSP;
import cadlabs.rdd.Flight;
import cadlabs.rdd.Path;
import org.apache.spark.api.java.JavaRDD;

public class SSSPMain extends AbstractSSSP {

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
    public SSSPMain(String URL, int numberNodes, int percentageOfConnections, String source, String destination) {
        super(URL, numberNodes, percentageOfConnections);
        this.source = source;
        this.destination = destination;
    }

    @Override
    protected Path run(JavaRDD<Flight> flights) {
        return new SSSP(this.source, this.destination, flights).run();
    }


    public static void main(String[] args) {
        new SSSPMain("local", 1000, 10, "Node1",
                "Node435").run();
    }
}
