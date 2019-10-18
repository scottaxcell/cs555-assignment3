package cs555.hadoop;

import java.text.SimpleDateFormat;
import java.util.Date;

public class Utils {
    private static final SimpleDateFormat SIMPLE_DATE_FORMAT = new SimpleDateFormat("HH:mm:ss.SSS");
    private static boolean debug = true;

    public static void out(Object o) {
        System.out.print(o);
    }

    public static void info(Object o) {
        info(o, true);
    }

    public static void info(Object o, boolean newLine) {
        if (newLine)
            System.out.println("INFO: " + o);
        else
            System.out.print("INFO: " + o);
    }

    public static void debug(Object o) {
        if (debug)
            System.out.println(String.format("\nDEBUG %s %s", SIMPLE_DATE_FORMAT.format(new Date()), o));
    }

    public static void error(Object o) {
        System.err.println("\nERROR: " + o);
    }

    public static String parseString(String string) {
        return string.trim();
    }

    public static int parseDelay(String string) {
        try {
            float delay = Float.parseFloat(string);
            return delay < 0 ? 0 : (int) delay;
        }
        catch (Exception e) {
            return 0;
        }
    }

    public static int sumDelays(String[] split) {
        return Utils.parseDelay(split[MainIndex.ARR_DELAY].trim()) +
            Utils.parseDelay(split[MainIndex.DEP_DELAY].trim()) +
            Utils.parseDelay(split[MainIndex.CARRIER_DELAY].trim()) +
            Utils.parseDelay(split[MainIndex.WEATHER_DELAY].trim()) +
            Utils.parseDelay(split[MainIndex.NAS_DELAY].trim()) +
            Utils.parseDelay(split[MainIndex.SECURITY_DELAY].trim()) +
            Utils.parseDelay(split[MainIndex.LATE_AIRCRAFT_DELAY].trim());
    }
}
