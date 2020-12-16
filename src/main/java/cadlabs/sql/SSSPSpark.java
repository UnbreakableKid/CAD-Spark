package cadlabs.sql;


import cadlabs.rdd.AbstractFlightAnalyser;
import cadlabs.rdd.Flight;

import cadlabs.rdd.Path;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.linalg.distributed.CoordinateMatrix;
import org.apache.spark.mllib.linalg.distributed.IndexedRow;
import org.apache.spark.mllib.linalg.distributed.IndexedRowMatrix;
import org.apache.spark.mllib.linalg.distributed.MatrixEntry;
import org.apache.spark.rdd.RDD;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;


/**
 * A sequential implementation of Dijkstra Single Source Shortest Path
 */
public class SSSPSpark extends AbstractFlightAnalyser<Path> {

    /**
     * Representation of absence of edge between two nodes
     */
    private static final double NOEDGE = Double.MAX_VALUE;

    /**
     * Representation of absence of predecessor for a given node in the current path
     */
    private static final int NOPREDECESSOR = -1;

    /**
     * The graph
     */
    private final CoordinateMatrix graph;

    private final IndexedRowMatrix iGraph;


    /**
     * The name of the source node (airport)
     */
    private final String sourceName;

    /**
     * The name of the destination node (airport)
     */
    private final String destinationName;


    public SSSPSpark(String source, String destination, JavaRDD<Flight> flights) {
        super(flights);
        this.sourceName = source;
        this.destinationName = destination;
        this.graph = buildGraph();
        this.iGraph = graph.toIndexedRowMatrix();

    }

    @Override
    public Path run() {
        // identifiers of the source and destination nodes
        int source = Flight.getAirportIdFromName(sourceName);
        int destination = Flight.getAirportIdFromName(destinationName);
        int nAirports = (int) Flight.getNumberAirports();

        int[] predecessor = new int[nAirports];
        double[] l = new double[nAirports];

        Arrays.fill(l, NOEDGE);
        Arrays.fill(predecessor, NOPREDECESSOR);

        List<Integer> toVisit = IntStream.range(0, nAirports).boxed().collect(Collectors.toList());
        toVisit.remove(source);

        l[source] = 0;
        for (Integer v : toVisit) {
            MatrixEntry e = getEdge(source, v);
            if (e != null) {
                l[(int) e.j()] = e.value();
                predecessor[(int) e.j()] = source;
            }
        }

        JavaPairRDD<Integer, Double> d = calculateD(l,source);
        for (IndexedRow e: iGraph.rows().toJavaRDD().collect()
        ) {
            System.out.println(e);

        }
        JavaRDD<IndexedRow> x = iGraph.rows().toJavaRDD();
        JavaPairRDD<Integer, Double> finalD = d;
        x.mapToPair(indexedRow -> finalD.join(indexedRow.index()));
        // Dijkstra's algorithm
        while (toVisit.size() > 0) {
            Tuple2<Integer, Double> min = findMin(d,source, toVisit);

            int u = min._1;
            toVisit.remove((Integer) u);

            System.out.println("Visiting " + u + ". Nodes left to visit: " + toVisit.size());

            for (Integer v : toVisit) {
                double newPath = l[u] + getWeight(u, v);
                if (l[v] > newPath) {
                    l[v] = newPath;
                    predecessor[v] = u;
                    d.unpersist();
                    d = calculateD(l, source).persist(StorageLevel.MEMORY_ONLY_2());

                }
            }
        }
        return new Path(source, destination, l[destination], predecessor);
    }

    private Tuple2<Integer, Double> findMin(JavaPairRDD<Integer, Double> d, int source, List<Integer> toVisit) {
        JavaPairRDD<Integer, Double> sorted =  // sort by value
                d.filter(v1 -> v1._1 != source).mapToPair(x -> x.swap()).sortByKey(true).
                        mapToPair(x -> x.swap()).filter(v1 -> toVisit.contains(v1._1));

        return sorted.take(1).get(0);
    }

    private JavaPairRDD<Integer, Double> calculateD(double[] dh, int source) {
         return this.flights.mapToPair(flight -> new Tuple2<>((int) flight.destInternalId, dh[(int) flight.destInternalId] )).reduceByKey((v1, v2) -> v1);

    }

    /**
     * Build the graph using Spark for convenience
     * @return The graph
     */
    private CoordinateMatrix buildGraph() {

        JavaPairRDD<Tuple2<Long, Long>, Tuple2<Double, Integer>> aux1 =
                this.flights.
                        mapToPair(
                                flight ->
                                        new Tuple2<>(
                                                new Tuple2<>(flight.origInternalId, flight.destInternalId),
                                                new Tuple2<>(flight.arrtime - flight.deptime < 0 ?
                                                        flight.arrtime - flight.deptime + 2400 :
                                                        flight.arrtime - flight.deptime, 1)));

        JavaPairRDD<Tuple2<Long, Long>, Tuple2<Double, Integer>> aux2 =
                aux1.reduceByKey((duration1, duration2) ->
                        new Tuple2<>(duration1._1 + duration2._1,
                                duration1._2 + duration2._2));

        JavaPairRDD<Tuple2<Long, Long>, Double> flightAverageDuration =
                aux2.mapToPair(flightDuration ->
                        new Tuple2<>(flightDuration._1, flightDuration._2._1 / flightDuration._2._2));

        JavaRDD<MatrixEntry> entries =
                flightAverageDuration.map(
                        flight -> new MatrixEntry(flight._1._1, flight._1._2, flight._2));

        CoordinateMatrix cm = new CoordinateMatrix(entries.rdd());
        cm.transpose();
        return cm;
    }


    /**
     * Obtain an edge from between origin and dest, if it exists.
     *
     * @return The edge (of type MatrixEntry), if it exists, null otherwise
     */
    private MatrixEntry getEdge(int origin, int dest) {
        for (MatrixEntry e : this.graph.entries().toJavaRDD().collect()) {
            if (e.i() == origin && e.j() == dest)
                return e;
        }
        return null;
    }

    /**
     * Obtain the weight of an edge from between origin and dest, if it exists.
     *
     * @return The weight of the edge, if the edge exists, NOEDGE otherwise
     */
    private double getWeight(int origin, int dest) {
        for (MatrixEntry e :  this.graph.entries().toJavaRDD().collect()) {
            if (e.i() == origin && e.j() == dest)
                return e.value();
        }
        return NOEDGE;
    }
}
