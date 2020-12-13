package cadlabs.rdd;

import java.util.ArrayList;
import java.util.List;

/**
 * Class that represents a path in the graph
 */
public class Path {

    /**
     * The list representation of the path
     */
    private final List<String> path = new ArrayList<>();

    /**
     * The weigth of the path between the two nodes
     */
    private double pathWeight;

    /**
     * Constructor given the source and destination node ids and the list of predecessors
     * @param source
     * @param destination
     * @param predecessor
     */
    public Path(long source, long destination, double pathWeight,  int[] predecessor) {
        for (int v =  (int) destination; v != source; v = predecessor[v])
            this.path.add(0, Flight.getAirportNameFromId(v));

        this.path.add(0, Flight.getAirportNameFromId((int) source));
        this.pathWeight = pathWeight;
    }

    /**
     * Obtain the path as a list of node names
     * @return The list of node names
     */
    public List<String> getPathAsList() {
        return this.path;
    }


    public double getWeight() {
        return this.pathWeight;
    }

    @Override
    public String toString() {
        String result = "";
        int last = this.path.size()-1;

        for (int i = 0; i < last; i++)
            result += this.path.get(i) + " -> ";

        return result + this.path.get(last);
    }
}
