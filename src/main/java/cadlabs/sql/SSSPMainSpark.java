package cadlabs.sql;

import cadlabs.AbstractSSSP;
import cadlabs.rdd.Flight;
import cadlabs.rdd.Path;
import org.apache.spark.api.java.JavaRDD;

import java.util.Scanner;

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
     *
     * @param URL                     URL of the Spark master
     * @param numberNodes             Number of nodes in the graph
     * @param percentageOfConnections To how many nodes (in percentage) a node is connected to.
     * @param source                  Source node
     * @param destination             Destination node
     */
    public SSSPMainSpark(String URL, int numberNodes, int percentageOfConnections, String source, String destination) {
        super(URL, numberNodes, percentageOfConnections);
        this.source = source;
        this.destination = destination;
    }


    @Override
    protected Path run(JavaRDD<Flight> flights) {

        return new SSSPSpark(this.source, this.destination, flights).run();
    }



    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);
        String input = scanner.nextLine();
        String[] x = input.split(" ");
        if (x[0].compareTo("info") == 0)
            new SSSPMainSpark("local", 10, 60, x[1],
                    x[2]).run();

        if (x[0].compareTo("time") == 0)
            new SSSPMainSpark("local", 10, 60, x[1],
                    x[2]).run();

    }
}
