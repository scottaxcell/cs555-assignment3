package cs555.hadoop;

public class MainIndex {
    public static final int YEAR = 0;
    public static final int MONTH = 1;
    public static final int DAY_OF_WEEK = 3;
    public static final int DEP_TIME = 4;

    public static final int ARR_DELAY = 14; // Difference in minutes between scheduled and actual arrival time. Early arrivals show negative numbers.
    public static final int DEP_DELAY = 15; // Difference in minutes between scheduled and actual departure time. Early departures show negative numbers.
    public static final int ORIGIN = 16; // iata
    public static final int DEST = 16; // iata


    public static final int CARRIER_DELAY = 24;
    public static final int WEATHER_DELAY = 25;
    public static final int NAS_DELAY = 26;
    public static final int SECURITY_DELAY = 27;
    public static final int LATE_AIRCRAFT_DELAY = 28;
}
