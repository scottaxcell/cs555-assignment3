package cs555.hadoop;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * What are the major hubs (busiest airports) in continental U.S.? Please list the top 10. Has there
 * been a change over the 21-year period covered by this dataset?
 */
public class QuestionThree {
    private static final String MAIN_MAP_ID = "MAIN_MAP_ID";
    private static final String AIRPORTS_MAP_ID = "AIRPORTS_";

    public static class MainMap extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] split = value.toString().split(",", -1);
            if (split.length != Constants.MAIN_SPLIT_LENGTH)
                return;

            Text iata = new Text(split[MainIndex.ORIGIN].trim());
            Utils.debug("main: " + iata + " -- " + MAIN_MAP_ID);
            context.write(iata, new Text(MAIN_MAP_ID));
        }
    }

    public static class AirportsMap extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] split = value.toString().split(",", -1);
            if (split.length != Constants.AIRPORTS_SPLIT_LENGTH)
                return;

            Text iata = new Text(split[AirportsIndex.IATA].replace("\"", "").trim());
            Text airport = new Text(split[AirportsIndex.AIRPORT].replace("\"", "").trim());
            Utils.debug("airports: " + iata + " -- " + airport);
            context.write(iata, airport);
        }
    }

    public static class Reducer extends org.apache.hadoop.mapreduce.Reducer<Text, Text, Text, Text> {
        Map<String, Long> iataToNumFlights;
        Map<String, String> iataToAirport;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            iataToNumFlights = new HashMap<>();
            iataToAirport = new HashMap<>();
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value : values) {
                if (value.toString().equals(MAIN_MAP_ID)) {
                    Utils.debug("reducer main: " + key + " -- " + value);
                    long count = iataToNumFlights.containsKey(key.toString()) ? iataToNumFlights.get(key.toString()) : 0;
                    iataToNumFlights.put(key.toString(), count + 1);
                }
                else {
                    Utils.debug("reducer airports: " + key + " -- " + value);
                    iataToAirport.putIfAbsent(key.toString(), value.toString());
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            iataToNumFlights.entrySet().stream()
                .sorted(Map.Entry.comparingByValue())
                .map(e -> String.format("%d\t(%s) %s", e.getValue(), e.getKey(), iataToAirport.get(e.getKey())))
                .forEach(s -> {
                    try {
                        context.write(new Text(s), new Text());
                    }
                    catch (IOException | InterruptedException ex) {
                        ex.printStackTrace();
                    }
                });

            super.cleanup(context);
        }
    }
}
