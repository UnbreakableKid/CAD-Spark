package cadlabs.sql;


import cadlabs.rdd.AbstractFlightAnalyser;
import cadlabs.rdd.Flight;

import cadlabs.rdd.Path;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.linalg.SparseVector;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.distributed.CoordinateMatrix;
import org.apache.spark.mllib.linalg.distributed.IndexedRowMatrix;
import org.apache.spark.mllib.linalg.distributed.MatrixEntry;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;
import scala.Tuple3;

import java.util.*;
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
    private CoordinateMatrix graph;

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

        double[] l = new double[nAirports];

        Arrays.fill(l, NOEDGE);

        List<Integer> toVisit = IntStream.range(0, nAirports).boxed().collect(Collectors.toList());
        toVisit.set(source, -1);

        l[source] = 0;
        for (Integer v : toVisit) {
            MatrixEntry e = getEdge(source, v);
            if (e != null) {
                l[(int) e.j()] = e.value();
            }
        }

        this.graph = null;

        JavaPairRDD<Integer, Tuple3<Double, Boolean, Integer>> d = calculateD(l, toVisit, source);

        JavaPairRDD<Integer, Vector> indexRdd = iGraph.rows().toJavaRDD().mapToPair(r -> new Tuple2<>((int)r.index(), r.vector())).persist(StorageLevel.MEMORY_ONLY()).cache();

        int visited = 1;
        // Dijkstra's algorithm
        while (visited < toVisit.size()) {
            Tuple2<Integer, Tuple3<Double, Boolean, Integer>> min = findMin(d, source).take(10).get(0);

            int u = min._1;
            toVisit.set(u, -1);
            visited++;

            JavaPairRDD<Integer, Tuple2<Tuple3<Double, Boolean, Integer>, Vector>> all = d.join(indexRdd);

            d = checkIfUpdated(d, all.collectAsMap(), u, min._2._1(), nAirports, toVisit);

        }

        int[] predec = new int[nAirports];

        assert d != null;
        for (Tuple2<Integer, Tuple3<Double, Boolean, Integer>> v : d.collect()) {
            predec[v._1] = v._2._3();
        }

        indexRdd.unpersist();
        return new Path(source, destination,getFinalWeight(d, destination) , predec);
    }

    private Double getFinalWeight(JavaPairRDD<Integer, Tuple3<Double, Boolean, Integer>> d, int destination){
        return d.filter(v1 -> v1._1 == destination).take(1).get(0)._2._1();

    }

    private JavaPairRDD<Integer, Tuple3<Double, Boolean, Integer>> findMin(JavaPairRDD<Integer, Tuple3<Double, Boolean, Integer>> d, int source) {
        JavaPairRDD<Double, Tuple3<Boolean, Integer, Integer>> sorted =  // sort by value <index, (6, boolean, predecssor)>
                    d.filter(v1 ->  v1._1 != source && !v1._2._2())
                        .mapToPair(x -> new Tuple2<>(x._2._1(),  new Tuple3<>(x._2._2(), x._2._3() ,x._1))).sortByKey(true);


        return sorted.mapToPair(x -> new Tuple2<>(x._2._3(), new Tuple3<>(x._1, x._2._1(), x._2._2())));
    }

    private JavaPairRDD<Integer, Tuple3<Double, Boolean, Integer>> calculateD(double[] dh, List<Integer> toVisit, int source) {
         return this.flights.mapToPair(flight -> new Tuple2<>((int) flight.destInternalId, new Tuple3<>(dh[(int) flight.destInternalId], toVisit.get((int) flight.destInternalId) == -1,
                 dh[(int) flight.destInternalId] != NOEDGE && flight.destInternalId != source? source : NOPREDECESSOR))).reduceByKey((v1, v2) -> v1);

    }

    /**
     *
     * @param d
     * @param all
     * @param minIndex
     * @param minValue
     * @param n number of airports
     * @param toVisit haven't visited
     * @return updated d
     */
    private JavaPairRDD<Integer, Tuple3<Double, Boolean, Integer>> checkIfUpdated(JavaPairRDD<Integer, Tuple3<Double, Boolean, Integer>> d, Map<Integer, Tuple2<Tuple3<Double, Boolean, Integer>, Vector>> all, int minIndex, double minValue, int n, List<Integer> toVisit) {

        double[] toChange = new double[n];
        Arrays.fill(toChange, Double.MAX_VALUE);

        for (Map.Entry<Integer, Tuple2<Tuple3<Double, Boolean, Integer>, Vector>> v : all.entrySet()) {
            if (toVisit.get(v.getKey()) != -1) {
                SparseVector z = v.getValue()._2.toSparse();
                int[] i = z.indices();
                int toCheck = -1;
                for (int j = 0; j < i.length; j++) {
                    if (i[j] == minIndex) {
                        toCheck = j;
                        break;
                    }
                }
                if (toCheck != -1) {
                    double x = z.values()[toCheck];
                    toChange[v.getKey()] = x;
                }
            }
        }
        return d.mapToPair(x -> new Tuple2<>(x._1, new Tuple3<>(toChange[x._1] + minValue <= x._2._1()? toChange[x._1] + minValue : x._2._1() , toVisit.get(x._1)==-1, toChange[x._1] + minValue < x._2._1() || toChange[x._1] + minValue == x._2._1()  ?   minIndex : x._2._3())));
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
        return cm.transpose();
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
