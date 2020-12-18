package cadlabs.sql;

import cadlabs.AbstractSSSP;
import cadlabs.rdd.Flight;
import cadlabs.rdd.Path;
import org.apache.commons.collections.map.MultiKeyMap;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.distributed.MatrixEntry;
import scala.Tuple2;

import java.util.*;

public class SSSPMainSpark extends AbstractSSSP {

    /**
     * Source node
     */
    private  String source;

    /**
     * Destination node
     */
    private  String destination;

    JavaRDD<MatrixEntry> toAdd;

    Map<Integer, List<Integer>> helper;

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
        this.helper = new HashMap<>();

    }


    @Override
    protected Path run(JavaRDD<Flight> flights) {
        return new SSSPSpark(this.source, this.destination, flights, toAdd).run();
    }



    public static void main(String[] args) {
        SSSPMainSpark z = new SSSPMainSpark("local", 10, 60, null, null);
        JavaSparkContext sc = new JavaSparkContext(z.spark.sparkContext());
        System.out.println("Type quit to exit spark");
        while (true) {
            Scanner scanner = new Scanner(System.in);
            String input = scanner.nextLine();
            String[] x = input.split(" ");
            if (x[0].compareTo("info") == 0) {
                z.destination = x[2];
                z.source = x[1];
                z.run();
            }


            if (x[0].compareTo("time") == 0)
            {
                List<String> data = Collections.singletonList(x[1] + " " + x[2] + " " + x[3]);
                JavaRDD<String> items = sc.parallelize(data,1);

                List<Integer> toput= new ArrayList<>();

                JavaRDD<MatrixEntry> test = items.map(v1 -> new MatrixEntry(Flight.getAirportIdFromName(v1.split(" ")[1]), Flight.getAirportIdFromName(v1.split(" ")[0]),
                    Integer.parseInt(v1.split(" ")[2])));

                int i = Flight.getAirportIdFromName(x[1]);
                int j = Flight.getAirportIdFromName(x[2]);


                if(z.toAdd == null){
                    z.toAdd = test;
                    toput.add(i);
                    z.helper.put(j, toput);
                }
                else {
                    if(z.helper.containsKey(j) ) {

                        toput = z.helper.get(j);


                        if(toput.contains(i)){
                            z.toAdd = z.toAdd.filter(v1 -> (v1.i() != j && v1.j() == i) || (v1.i() == j && v1.j() != i) || (v1.i() != j && v1.j() != i) ).union(test);
                        }
                        else{
                            z.toAdd = z.toAdd.union(test);
                            toput.add(i);
                            z.helper.put(j, toput);

                        }

                    } else {
                        z.toAdd = z.toAdd.union(test);
                        toput.add(i);
                        z.helper.put(j, toput);
                    }


                }


                System.out.println("Added " +x[1] +"("+Flight.getAirportIdFromName(x[1])+") -> " + x[2] + "(" + Flight.getAirportIdFromName(x[2]) + ") with weight: " + x[3] );
            }

            if(x[0].compareTo("quit")==0)
                break;
        }

        z.spark.stop();


    }
}
