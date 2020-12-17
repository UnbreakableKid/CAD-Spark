package cadlabs.seq;

import cadlabs.AbstractSSSP;
import cadlabs.rdd.Flight;
import cadlabs.rdd.Path;
import cadlabs.sql.SSSPMainSpark;
import org.apache.spark.api.java.JavaRDD;

import java.util.Scanner;

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
//        CoordinateMatrix cm = buildGraph(flights);
//        JavaPairRDD<Integer, Integer> x = new calculateD(flights, cm, Flight.getAirportIdFromName(this.source)).run();
        return new SSSP(this.source, this.destination, flights).run();
    }


    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);
        String input = scanner.nextLine();
        String[] x = input.split(" ");
        if (x[0].compareTo("info") == 0)
            new SSSPMain("local", 10, 60, x[1],
                    x[2]).run();

    }
}