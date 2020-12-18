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
    private String source;

    /**
     * Destination node
     */
    private String destination;


    /**
     * Constructor
     *
     * @param URL                     URL of the Spark master
     * @param numberNodes             Number of nodes in the graph
     * @param percentageOfConnections To how many nodes (in percentage) a node is connected to.
     * @param source                  Source node
     * @param destination             Destination node
     */
    public SSSPMain(String URL, int numberNodes, int percentageOfConnections, String source, String destination, Boolean flightJavaRDD) {
        super(URL, numberNodes, percentageOfConnections, flightJavaRDD);
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

        SSSPMain z;

        if(args.length > 0 && args[0].compareTo("1") == 0) {

            z = new SSSPMain("local", 1000, 25, null, null, true);
        }
        else z = new SSSPMain("local", 100, 90, null, null,false);

        while (true) {
            Scanner scanner = new Scanner(System.in);
            String input = scanner.nextLine();
            String[] x = input.split(" ");
            if (x[0].compareTo("info") == 0) {
                z.destination = x[2];
                z.source = x[1];
                z.run();

                if (x[0].compareTo("quit") == 0)
                    break;
            }

        }
        z.spark.stop();
    }
}