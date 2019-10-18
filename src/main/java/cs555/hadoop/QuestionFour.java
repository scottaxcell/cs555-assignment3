package cs555.hadoop;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Which cities experience the most weather related delays? Please list the top 10.
 */
public class QuestionFour {
    public static class MainMapper extends org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] split = value.toString().split(",", -1);
            if (split.length != Constants.MAIN_SPLIT_LENGTH)
                return;

            Text iata = new Text(split[MainIndex.ORIGIN].trim());
            int delay = Utils.parseDelay(split[MainIndex.WEATHER_DELAY].trim());
            if (delay > 0) {
                Utils.debug("main: " + iata + " -- " + delay);
                context.write(iata, new Text(Constants.MAIN_MAP_ID));
            }
        }
    }

    public static class AirportsMapper extends org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] split = value.toString().split(",", -1);
            if (split.length != Constants.AIRPORTS_SPLIT_LENGTH)
                return;

            Text iata = new Text(split[AirportsIndex.IATA].replace("\"", "").trim());
            Text city = new Text(split[AirportsIndex.CITY].replace("\"", "").trim());
            Utils.debug("airports: " + iata + " -- " + city);
            context.write(iata, city);
        }
    }

    public static class Reducer extends org.apache.hadoop.mapreduce.Reducer<Text, Text, Text, Text> {
        private Map<String, Long> iataToNumDelays;
        private Map<String, String> iataToCity;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            iataToNumDelays = new HashMap<>();
            iataToCity = new HashMap<>();
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value : values) {
                Utils.debug("reducer: " + key + " -- " + value);
                if (value.toString().equals(Constants.MAIN_MAP_ID)) {
                    long count = iataToNumDelays.containsKey(key.toString()) ? iataToNumDelays.get(key.toString()) : 0;
                    iataToNumDelays.put(key.toString(), count + 1);
                }
                else {
                    iataToCity.putIfAbsent(key.toString(), value.toString());
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            iataToNumDelays.entrySet().stream()
                .sorted(Map.Entry.comparingByValue())
                .map(e -> String.format("%d\t(%s) %s", e.getValue(), e.getKey(), iataToCity.get(e.getKey())))
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
