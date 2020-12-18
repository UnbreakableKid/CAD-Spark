package cadlabs;

import cadlabs.rdd.DatasetGenerator;
import cadlabs.rdd.Flight;
import cadlabs.rdd.Path;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

public abstract class AbstractSSSP {

    /**
     * Spark session
     */
    protected final SparkSession spark;

    /**
     * Number of nodes in the graph
     */
    private final int numberNodes;

    /**
     * To how many nodes (in percentage) a node is connected to.
     * Value between 0 and 100
     */
    private final int percentageOfConnections;

    private  JavaRDD<Flight> flights;

    /**
     * Constructor
     * @param URL URL of the Spark master
     * @param numberNodes Number of nodes in the graph
     * @param percentageOfConnections To how many nodes (in percentage) a node is connected to.
     */
    public AbstractSSSP(String URL, int numberNodes, int percentageOfConnections, Boolean flights) {

        this.spark = SparkSession.builder().
                appName("FlightAnalyser").
                master(URL).
                getOrCreate();

        this.numberNodes = numberNodes;
        this.percentageOfConnections = Math.min(100, Math.max(0, percentageOfConnections));

        if(flights) {
            JavaRDD<String> textFile = spark.read().textFile("data/flights.csv").javaRDD();
            this.flights = textFile.map(Flight::parseFlight).cache();

        }
        else this.flights = new DatasetGenerator(this.numberNodes, this.percentageOfConnections, 0).
                build(this.spark.sparkContext());;
        // only error messages are logged from this point onward
        // comment (or change configuration) if you want the entire log
        spark.sparkContext().setLogLevel("ERROR");
    }

    /**
     * Apply the computation of the RDD of flights
     * @param flights The generated dataset of flights
     * @return
     */
    protected abstract Path run(JavaRDD<Flight> flights);


    /**
     * Trigger the generation of the dataset and the execution of the computation
     */
    public void run() {

        long start = System.currentTimeMillis();
        Path result = run(flights);
        long elapsed = System.currentTimeMillis() - start;

        System.out.println("Route: " + result + " with time: " + result.getWeight() + "m"+ "\nComputed in " + elapsed + " ms.");

        // terminate the session
    }

}
