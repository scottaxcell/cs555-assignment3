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
}
