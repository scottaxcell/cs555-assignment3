package cs555.hadoop;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Do East or West coast airports have more delays? Include details to substantiate your
 * analysis
 */
public class QuestionSix {
    private static List<String> westCoastStates = new ArrayList<>();
    private static List<String> eastCoastStates = new ArrayList<>();

    static {
        westCoastStates.add("WA");
        westCoastStates.add("OR");
        westCoastStates.add("CA");

        eastCoastStates.add("ME");
        eastCoastStates.add("NH");
        eastCoastStates.add("MA");
        eastCoastStates.add("RI");
        eastCoastStates.add("CT");
        eastCoastStates.add("NJ");
        eastCoastStates.add("DE");
        eastCoastStates.add("MD");
        eastCoastStates.add("DC");
        eastCoastStates.add("NY");
        eastCoastStates.add("VA");
        eastCoastStates.add("NC");
        eastCoastStates.add("SC");
        eastCoastStates.add("GA");
        eastCoastStates.add("FL");
    }

    public static class MainMapper extends org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] split = value.toString().split(",", -1);
            if (split.length != Constants.MAIN_SPLIT_LENGTH)
                return;

            Text iata = new Text(split[MainIndex.ORIGIN].trim());
            if (Utils.sumDelays(split) > 0) {
                context.write(iata, new Text(Constants.MAIN_MAP_ID));
            }
        }
    }

    public static class AirportsMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] split = value.toString().split(",", -1);
            if (split.length != Constants.AIRPORTS_SPLIT_LENGTH)
                return;

            Text iata = new Text(split[AirportsIndex.IATA].replace("\"", "").trim());
            Text state = new Text(split[AirportsIndex.STATE].replace("\"", "").trim());
            context.write(iata, state);
        }
    }

    public static class Reducer extends org.apache.hadoop.mapreduce.Reducer<Text, Text, Text, Text> {
        private Map<String, Long> iataToNumDelays;
        private Map<String, String> iataToState;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            iataToNumDelays = new HashMap<>();
            iataToState = new HashMap<>();
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value : values) {
                if (value.toString().equals(Constants.MAIN_MAP_ID)) {
                    long count = iataToNumDelays.containsKey(key.toString()) ? iataToNumDelays.get(key.toString()) : 0;
                    iataToNumDelays.put(key.toString(), count + 1);
                }
                else {
                    iataToState.putIfAbsent(key.toString(), value.toString());
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            long numWestCoastDelays = 0;
            long numEastCoastDelays = 0;

            Set<String> westCoastAirports = new HashSet<>();
            Set<String> eastCoastAirports = new HashSet<>();

            for (Map.Entry<String, Long> entry : iataToNumDelays.entrySet()) {
                String state = iataToState.get(entry.getKey());
                if (westCoastStates.contains(state)) {
                    numWestCoastDelays += entry.getValue();
                    westCoastAirports.add(entry.getKey());
                }
                else if (eastCoastStates.contains(state)) {
                    numEastCoastDelays += entry.getValue();
                    eastCoastAirports.add(entry.getKey());
                }
            }

            context.write(new Text("Number of West coast delays: " + numWestCoastDelays), new Text());
            context.write(new Text("Number of West coast airports: " + westCoastAirports.size()), new Text());
            context.write(new Text(), new Text());
            context.write(new Text("Number of East coast delays: " + numEastCoastDelays), new Text());
            context.write(new Text("Number of East coast airports: " + eastCoastAirports.size()), new Text());

            context.write(new Text(), new Text());
            context.write(new Text("--------"), new Text());
            context.write(new Text(), new Text());

            String westCoastStates = QuestionSix.westCoastStates.stream().
                map(Object::toString).
                collect(Collectors.joining(",")).toString();
            context.write(new Text("West coast states: " + westCoastStates), new Text());
            String eastCoastStates = QuestionSix.westCoastStates.stream().
                map(Object::toString).
                collect(Collectors.joining(",")).toString();
            context.write(new Text("East coast states: " + eastCoastStates), new Text());

            super.cleanup(context);
        }
    }
}
