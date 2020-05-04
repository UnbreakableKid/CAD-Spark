package cadlabs.rdd;

import java.util.HashMap;
import java.util.Map;

public class Flight {

	/**
	 * day of Month
	 */
	final String dofM;
	
	/**
	 * day of Week
	 */
	final String dofW;
	
	/**
	 * The carrier
	 */
	final String carrier;
	
	/**
	 * Airplane tail number
	 */
	final String tailnum;
	
	/**
	 * Flight number
	 */
	final int flnum;
	
	
	/**
	 * Identifier of the airport of origin
	 */
	final long org_id;
	
	
	/**
	 * Name of the airport of origin
	 */
	final String origin;
	
	/**
	 * Identifier of the airport of destination
	 */
	final long dest_id;
	
	/**
	 * Name of the airport of destination
	 */
	final String dest;
	
	/**
	 * Scheduled departure time
	 */
	final double crsdeptime;
	
	/**
	 * Departure time
	 */
	final double deptime;
	
	/**
	 * Delay at departure
	 */
	final double depdelaymins;
	
	/**
	 * Scheduled arrival time
	 */
	final double crsarrtime;
	
	/**
	 * Arrival time
	 */
	final double arrtime;
	
	/**
	 * Delay at arrival
	 */
	final double arrdelay;
	
	/**
	 * Scheduled elapsed (flight) time
	 */
	final double crselapsedtime;
	
	/**
	 * Distance between airports
	 */
	final int dist;
	
	/**
	 * Identifier in a range [0..numberAirports-1] of the airport of arrival
	 */
	final long origInternalId; 
	
	
	/**
	 * Identifier in a range [0..numberAirports-1] of the airport of destination
	 */
	final long destInternalId; 
	
	
	/**
	 * Map of airport identifier into internal identifier
	 */
	private static final Map<Long, Integer> airports = new HashMap<Long, Integer>();
	
	
	/**
	 * Map of airport internal identifier into airport name
	 */
	private static final Map<Integer, String> airportsRev = new HashMap<Integer, String>();
	
	/**
	 * Map of airport name into internal identifier
	 */
	private static final Map<String, Integer> airportsByName = new HashMap<String, Integer>();
	
	
	private static int internalIds = 0;
	

	public Flight(String dofM, String dofW, String carrier, String tailnum, int flnum, long org_id, String origin,
			long dest_id, String dest, double crsdeptime, double deptime, double depdelaymins, double crsarrtime,
			double arrtime, double arrdelay, double crselapsedtime, int dist) {

		this.dofM = dofM;
		this.dofW = dofW;
		this.carrier = carrier;
		this.tailnum = tailnum;
		this.flnum = flnum;
		this.org_id = org_id;
		this.origin = origin;
		this.dest_id = dest_id;
		this.dest = dest;
		this.crsdeptime = crsdeptime;
		this.deptime = deptime;
		this.depdelaymins = depdelaymins;
		this.crsarrtime = crsarrtime;
		this.arrtime = arrtime;
		this.arrdelay = arrdelay;
		this.crselapsedtime = crselapsedtime;
		this.dist = dist;
		
		this.origInternalId = internalId(this.org_id, this.origin);
		this.destInternalId = internalId(this.dest_id, this.dest);

	}

	static Flight parseFlight(String line) {
		String[] data = line.split(",");
		return new Flight(data[0], data[1], data[2], data[3], Integer.parseInt(data[4]), Long.parseLong(data[5]),
				data[6], Long.parseLong(data[7]), data[8], Double.parseDouble(data[9]), Double.parseDouble(data[10]),
				Double.parseDouble(data[11]), Double.parseDouble(data[12]),
				data[13].equals("") ? 0 : Double.parseDouble(data[13]),
				data[14].equals("") ? 0 : Double.parseDouble(data[14]), Double.parseDouble(data[15]),
				Integer.parseInt(data[16]));
	
	}
	
	private static long internalId(long airport, String name)  {
		synchronized (airports) {
			Integer id = airports.get(airport);
			if (id == null) {
				id = internalIds++;
				airports.put(airport, id);
				airportsRev.put(id, name);
				airportsByName.put(name, id);
			}
			return id;
		}
	}
}