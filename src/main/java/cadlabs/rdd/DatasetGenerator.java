package cadlabs.rdd;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;


import java.util.*;

public class DatasetGenerator {

    /**
     * Number of nodes in the graph
     */
    private final int numberNodes;

    /**
     * To how many nodes (in percentage) a node is connected to.
     * Value between 0 and 100
     */
    private final int percentageOfConnections;

    /**
     * The random number generator
     */
    private final Random randomizer;


    /**
     * Assumming a grid of numberNodes x numberNodes
     */
    private Map<Integer, ImmutablePair<Integer, Integer>> coordinates = new HashMap<>();

     /**
     * Constructor
     * @param numberNodes Number of nodes in the graph
     * @param percentageOfConnections To how many nodes (in percentage) a node is connected to.
     * @param seed Seed for the random number generator
     */
    public DatasetGenerator(int numberNodes, int percentageOfConnections, int seed) {
        this.numberNodes = numberNodes;
        this.percentageOfConnections = numberNodes*Math.min(100, Math.max(0, percentageOfConnections))/100;
        this.randomizer = new Random(seed);
    }

    /**
     * Build a dataset of flights
     *
     * @param sc The spark context
     * @return The dataset as ad JavaRDD of Flight
     */
    public JavaRDD<Flight> build(SparkContext sc) {

        for (int i = 0; i < numberNodes; i++)
            coordinates.put(i, new ImmutablePair<>(randomizer.nextInt(numberNodes), randomizer.nextInt(numberNodes)));

        List<Flight> list = new ArrayList<>();
        for (int i = 0; i < numberNodes; i++) {

            list.add(newNode(i, (i+1)%this.numberNodes));
            for (int j = 0; j < this.percentageOfConnections; j++) {
                int dest = randomizer.nextInt(numberNodes);
                list.add(newNode(i, dest));
            }
        }

        return new JavaSparkContext(sc).parallelize(list);
    }

    private Flight newNode(int source, int dest) {


        int deptime = randomizer.nextInt(2400);
        int arrtime = weight(source, dest) + deptime;

        return new Flight(
                "dofm",
                "dofW",
                "carrier",
                "tailnum",
                randomizer.nextInt(),
                source,
                nodeName(source),
                dest,
                nodeName(dest),
                deptime,
                deptime,
                0,
                arrtime,
                arrtime,
                0,
                (arrtime - deptime),
                weight(source, dest));
    }

    private int weight(int s, int d) {
        ImmutablePair<Integer, Integer> scoord = coordinates.get(s);
        ImmutablePair<Integer, Integer> dcoord = coordinates.get(d);

        return ((int) Math.sqrt(Math.pow(scoord.left - dcoord.left, 2) +  Math.pow(scoord.right - dcoord.right, 2))) % 2400;
    }

    private String nodeName(int i) {
        return "Node"+i;
    }

}
