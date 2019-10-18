package cs555.hadoop;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * What are the major hubs (busiest airports) in continental U.S.? Please list the top 10. Has there
 * been a change over the 21-year period covered by this dataset?
 */
public class QuestionThree {
    public static class MainMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] split = value.toString().split(",", -1);
            if (split.length != Constants.MAIN_SPLIT_LENGTH)
                return;

            if (isHawaiiAirport(split[MainIndex.ORIGIN]))
                return;

            Text iata = new Text(split[MainIndex.ORIGIN].trim());
            context.write(iata, new Text(Constants.MAIN_MAP_ID));
        }
    }

    public static class AirportsMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] split = value.toString().split(",", -1);
            if (split.length != Constants.AIRPORTS_SPLIT_LENGTH)
                return;

            Text iata = new Text(split[AirportsIndex.IATA].replace("\"", "").trim());
            Text airport = new Text(split[AirportsIndex.AIRPORT].replace("\"", "").trim());
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
                if (value.toString().equals(Constants.MAIN_MAP_ID)) {
                    long count = iataToNumFlights.containsKey(key.toString()) ? iataToNumFlights.get(key.toString()) : 0;
                    iataToNumFlights.put(key.toString(), count + 1);
                }
                else {
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

    private static List<String> hawaiiAirports = new ArrayList<>();

    static {
        hawaiiAirports.add("BKH");
        hawaiiAirports.add("HDH");
        hawaiiAirports.add("HIK");
        hawaiiAirports.add("HKP");
        hawaiiAirports.add("HNL");
        hawaiiAirports.add("HNM");
        hawaiiAirports.add("HPV");
        hawaiiAirports.add("ITO");
        hawaiiAirports.add("JHM");
        hawaiiAirports.add("JRF");
        hawaiiAirports.add("KOA");
        hawaiiAirports.add("LIH");
        hawaiiAirports.add("LNY");
        hawaiiAirports.add("LUP");
        hawaiiAirports.add("MKK");
        hawaiiAirports.add("MUE");
        hawaiiAirports.add("NAX");
        hawaiiAirports.add("NGF");
        hawaiiAirports.add("OGG");
        hawaiiAirports.add("PAK");
        hawaiiAirports.add("QKV");
        hawaiiAirports.add("UPP");
        hawaiiAirports.add("WKL");
    }

    private static boolean isHawaiiAirport(String iata) {
        return hawaiiAirports.contains(iata);
    }


    public static class YearMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] split = value.toString().split(",", -1);
            if (split.length != Constants.MAIN_SPLIT_LENGTH)
                return;

            if (isHawaiiAirport(split[MainIndex.ORIGIN]))
                return;

            IntWritable year = new IntWritable(Integer.parseInt(split[MainIndex.YEAR].trim()));
            context.write(year, new IntWritable(1));
        }
    }

    public static class YearReducer extends org.apache.hadoop.mapreduce.Reducer<IntWritable, IntWritable, Text, Text> {
        @Override
        protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            long sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            context.write(new Text(String.valueOf(key.get())), new Text(String.valueOf(sum)));
        }
    }
}
