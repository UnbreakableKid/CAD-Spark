package cadlabs.sql;


import cadlabs.rdd.AbstractFlightAnalyser;
import cadlabs.rdd.Flight;

import cadlabs.rdd.Path;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.linalg.SparseVector;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.distributed.CoordinateMatrix;
import org.apache.spark.mllib.linalg.distributed.IndexedRowMatrix;
import org.apache.spark.mllib.linalg.distributed.MatrixEntry;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;
import scala.Tuple3;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
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
    private List<MatrixEntry> graph;

    private IndexedRowMatrix iGraph;


    /**
     * The name of the source node (airport)
     */
    private final String sourceName;

    /**
     * The name of the destination node (airport)
     */
    private final String destinationName;


    public SSSPSpark(String source, String destination, JavaRDD<Flight> flights, JavaRDD<MatrixEntry> toAdd) {
        super(flights);
        this.sourceName = source;
        this.destinationName = destination;
        if (toAdd == null) this.graph = buildGraph();

        else this.graph = buildGraph(toAdd);


    }



    @Override
    public Path run() {
        // identifiers of the source and destination nodes
        int source = Flight.getAirportIdFromName(sourceName);
        int destination = Flight.getAirportIdFromName(destinationName);
        int nAirports = (int) Flight.getNumberAirports();

        double[] ltemp = new double[nAirports];

        Arrays.fill(ltemp, NOEDGE);

        List<Integer> toVisit = IntStream.range(0, nAirports).boxed().collect(Collectors.toList());
        toVisit.set(source, -1);

        ltemp[source] = 0;
        for (Integer v : toVisit) {
            MatrixEntry e = getEdge(source, v);
            if (e != null) {
                ltemp[(int) e.j()] = e.value();
            }
        }

        this.graph = null;

        JavaPairRDD<Integer, Vector> indexRdd = iGraph.rows().toJavaRDD().mapToPair(r -> new Tuple2<>((int)r.index(), r.vector())).cache();

        JavaPairRDD<Integer, Tuple3<Double, Boolean, Integer>> l = calculateD(ltemp, source).cache();


        int visited = 1;
        // Dijkstra's algorithm
        while (visited < toVisit.size()) {

            Tuple2<Double, Integer> min = findMin(l, source);

            int u = min._2;
            toVisit.set(u, -1);
            visited++;

            JavaPairRDD<Integer, Tuple2<Tuple3<Double, Boolean, Integer>, Vector>> all = l.join(indexRdd);

            l = checkIfUpdated(all, u, min._1, toVisit);

        }
        indexRdd.unpersist();

        int[] predec = new int[nAirports];

        for (Tuple2<Integer, Tuple3<Double, Boolean, Integer>> v : l.collect()) {
            predec[v._1] = v._2._3();
        }

        l.unpersist();


        return new Path(source, destination,getFinalWeight(l, destination) , predec);
    }


    private Double getFinalWeight(JavaPairRDD<Integer, Tuple3<Double, Boolean, Integer>> l, int destination){
        return l.filter(v1 -> v1._1 == destination).take(1).get(0)._2._1();

    }

    private Tuple2<Double, Integer> findMin(JavaPairRDD<Integer, Tuple3<Double, Boolean, Integer>> l, int source) {
       return  // sort by value <index, (6, boolean, predecssor)>
                    l.filter(v1 ->  v1._1 != source && !v1._2._2())
                        .mapToPair(x -> new Tuple2<>(x._2._1(),x._1)).sortByKey(true).first();
    }

    private JavaPairRDD<Integer, Tuple3<Double, Boolean, Integer>> calculateD(double[] dh, int source) {
         return this.flights.mapToPair(flight -> new Tuple2<>((int) flight.destInternalId, new Tuple3<>(dh[(int) flight.destInternalId], flight.destInternalId == source,
                 dh[(int) flight.destInternalId] != NOEDGE && flight.destInternalId != source? source : NOPREDECESSOR))).reduceByKey((v1, v2) -> v1);

    }

    /**
     *
     * @param all
     * @param minIndex
     * @param minValue
     * @param toVisit haven't visited
     * @return updated l
     */
    private JavaPairRDD<Integer, Tuple3<Double, Boolean, Integer>> checkIfUpdated(JavaPairRDD<Integer, Tuple2<Tuple3<Double, Boolean, Integer>, Vector>> all, int minIndex, double minValue, List<Integer> toVisit) {


        return all.mapToPair(x -> new Tuple2<>(x._1, new Tuple3<>(x._2._2.toDense().values()[minIndex] != 0?
                x._2._2.toDense().values()[minIndex]  + minValue <= x._2._1._1() ?
                        x._2._2.toDense().values()[minIndex]  + minValue : x._2._1._1()
                : x._2._1._1() ,
                toVisit.get(x._1) == -1,

                x._2._2.toDense().values()[minIndex] != 0?
                        x._2._2.toDense().values()[minIndex]  + minValue <= x._2._1._1() ? minIndex : x._2._1._3()
                :  x._2._1._3())));
    }
        /**
         * Build the graph using Spark for convenience
         * @return The graph
         * @param toAdd
         */
    private List<MatrixEntry> buildGraph(JavaRDD<MatrixEntry> toAdd) {

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

//        JavaRDD<MatrixEntry> x = null;
//        entries = entries.map(v1 -> {
//
//            for (Map.Entry<Integer, List<Integer>> i: helper.entrySet()) {
//
//                x.filter((matrixEntry) -> i.getKey().compareTo((int) v1.i()) == 0 && i.getValue().contains((int) v1.j()));
//
//        }
//            return x;
//        }
//
//        );

        JavaPairRDD<Tuple2<Long, Long>, Double> fixed = toAdd.union(entries).mapToPair(matrixEntry -> new Tuple2<>(new Tuple2<>(matrixEntry.i(), matrixEntry.j()), matrixEntry.value())).reduceByKey((v1, v2) -> v1 );

        entries = fixed.map(v1 -> new MatrixEntry(v1._1._1, v1._1._2, v1._2));

        CoordinateMatrix cm = new CoordinateMatrix(entries.rdd());
        this.iGraph =  cm.transpose().toIndexedRowMatrix();

        return entries.collect();
    }
    private List<MatrixEntry> buildGraph() {

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
        this.iGraph =  cm.transpose().toIndexedRowMatrix();

        return entries.collect();
    }

    /**
     * Obtain an edge from between origin and dest, if it exists.
     *
     * @return The edge (of type MatrixEntry), if it exists, null otherwise
     */
    private MatrixEntry getEdge(int origin, int dest) {
        for (MatrixEntry e : this.graph) {
            if (e.i() == origin && e.j() == dest)
                return e;
        }
        return null;
    }

}
