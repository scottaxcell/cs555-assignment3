package cs555.hadoop;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.*;

/**
 * What are the major hubs (busiest airports) in continental U.S.? Please list the top 10. Has there
 * been a change over the 21-year period covered by this dataset?
 */
public class QuestionThree {

    private static List<String> stateAbbrevs = new ArrayList<>();

    static {
        stateAbbrevs.add("AL");
        stateAbbrevs.add("AK");
        stateAbbrevs.add("AZ");
        stateAbbrevs.add("AR");
        stateAbbrevs.add("CA");
        stateAbbrevs.add("CO");
        stateAbbrevs.add("CT");
        stateAbbrevs.add("DC");
        stateAbbrevs.add("DE");
        stateAbbrevs.add("FL");
        stateAbbrevs.add("GA");
//        stateAbbrevs.add("HI");
        stateAbbrevs.add("ID");
        stateAbbrevs.add("IL");
        stateAbbrevs.add("IN");
        stateAbbrevs.add("IA");
        stateAbbrevs.add("KS");
        stateAbbrevs.add("KY");
        stateAbbrevs.add("LA");
        stateAbbrevs.add("ME");
        stateAbbrevs.add("MD");
        stateAbbrevs.add("MA");
        stateAbbrevs.add("MI");
        stateAbbrevs.add("MN");
        stateAbbrevs.add("MS");
        stateAbbrevs.add("MO");
        stateAbbrevs.add("MT");
        stateAbbrevs.add("NE");
        stateAbbrevs.add("NV");
        stateAbbrevs.add("NH");
        stateAbbrevs.add("NJ");
        stateAbbrevs.add("NM");
        stateAbbrevs.add("NY");
        stateAbbrevs.add("NC");
        stateAbbrevs.add("ND");
        stateAbbrevs.add("OH");
        stateAbbrevs.add("OK");
        stateAbbrevs.add("OR");
        stateAbbrevs.add("PA");
        stateAbbrevs.add("RI");
        stateAbbrevs.add("SC");
        stateAbbrevs.add("SD");
        stateAbbrevs.add("TN");
        stateAbbrevs.add("TX");
        stateAbbrevs.add("UT");
        stateAbbrevs.add("VT");
        stateAbbrevs.add("VA");
        stateAbbrevs.add("WA");
        stateAbbrevs.add("WV");
        stateAbbrevs.add("WI");
        stateAbbrevs.add("WY");
    }

    private static boolean isContinentalState(String state) {
        return stateAbbrevs.contains(state);
    }

    public static class MainMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] split = value.toString().split(",", -1);
            if (split.length != Constants.MAIN_SPLIT_LENGTH)
                return;

            Text iata = new Text(split[MainIndex.ORIGIN].trim());
            context.write(iata, new Text(Constants.MAIN_MAP_ID + split[MainIndex.YEAR].trim()));
        }
    }

    public static class AirportsMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] split = value.toString().split(",", -1);
            if (split.length != Constants.AIRPORTS_SPLIT_LENGTH)
                return;

            Text iata = new Text(split[AirportsIndex.IATA].replace("\"", "").trim());
            Text airportAndState = new Text(split[AirportsIndex.AIRPORT].replace("\"", "").trim() + "," +
                split[AirportsIndex.STATE].replace("\"", "").trim());
            context.write(iata, airportAndState);
        }
    }

    public static class Reducer extends org.apache.hadoop.mapreduce.Reducer<Text, Text, Text, Text> {
        private Map<String, String> iataToAirport;
        private Map<String, String> iataToState;
        private Map<String, Map<String, Long>> yearToIataToNumFlights;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            yearToIataToNumFlights = new HashMap<>();
            iataToAirport = new HashMap<>();
            iataToState = new HashMap<>();
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String iata = key.toString();

            for (Text value : values) {
                if (value.toString().startsWith(Constants.MAIN_MAP_ID)) {
                    String yearStr = value.toString().replaceAll(Constants.MAIN_MAP_ID, "");
                    if (yearToIataToNumFlights.containsKey(yearStr)) {
                        Map<String, Long> iataToNumFlights = yearToIataToNumFlights.get(yearStr);
                        if (iataToNumFlights.containsKey(iata)) {
                            long count = iataToNumFlights.get(iata);
                            iataToNumFlights.put(iata, count + 1);
                        }
                        else {
                            iataToNumFlights.put(iata, 0L);
                        }
                    }
                    else {
                        yearToIataToNumFlights.put(yearStr, new HashMap<>());
                        yearToIataToNumFlights.get(yearStr).put(iata, 0L);
                    }
                }
                else {
                    String[] split = value.toString().split(",");
                    iataToAirport.putIfAbsent(iata, split[0]);
                    iataToState.putIfAbsent(iata, split[1]);
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            // Part 1
            //-------
            context.write(new Text("Part 1"), new Text());
            context.write(new Text("======"), new Text());

            Map<String, Long> iataToTotalNumFlights = new HashMap<>();

            for (Map<String, Long> iataToNumFlights : yearToIataToNumFlights.values()) {
                for (Map.Entry<String, Long> e : iataToNumFlights.entrySet()) {
                    long count = iataToTotalNumFlights.containsKey(e.getKey()) ? iataToTotalNumFlights.get(e.getKey()) : 0;
                    iataToTotalNumFlights.put(e.getKey(), count + e.getValue());
                }
            }

            iataToTotalNumFlights.entrySet().stream()
                .filter(e -> isContinentalState(getStateFromIata(e.getKey())))
                .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
                .limit(10)
                .map(e -> String.format("%d\t(%s) %s", e.getValue(), e.getKey(), iataToAirport.get(e.getKey())))
                .forEach(s -> {
                    try {
                        context.write(new Text(s), new Text());
                    }
                    catch (IOException | InterruptedException ex) {
                        ex.printStackTrace();
                    }
                });

            // Part 2
            //-------
            context.write(new Text(), new Text());
            context.write(new Text("Part 2"), new Text());
            context.write(new Text("======"), new Text());

            yearToIataToNumFlights.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .forEach(entry -> {
                    String year = entry.getKey();
                    try {
                        context.write(new Text(), new Text());
                        context.write(new Text(year), new Text());
                        context.write(new Text("----"), new Text());
                    }
                    catch (IOException | InterruptedException ex) {
                        ex.printStackTrace();
                    }
                    Map<String, Long> iataToNumFlights = entry.getValue();
                    iataToNumFlights.entrySet().stream()
                        .filter(e -> isContinentalState(getStateFromIata(e.getKey())))
                        .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
                        .limit(10)
                        .map(e -> String.format("%d\t(%s) %s", e.getValue(), e.getKey(), iataToAirport.get(e.getKey())))
                        .forEach(s -> {
                            try {
                                context.write(new Text(s), new Text());
                            }
                            catch (IOException | InterruptedException ex) {
                                ex.printStackTrace();
                            }
                        });
                });

            super.cleanup(context);
        }

        private String getStateFromIata(String iata) {
            return iataToState.get(iata) != null ? iataToState.get(iata) : "";
        }
    }
}
