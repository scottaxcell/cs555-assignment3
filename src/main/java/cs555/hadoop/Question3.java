package cs555.hadoop;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Arrays;

/**
 * What are the major hubs (busiest airports) in continental U.S.? Please list the top 10. Has there
 * been a change over the 21-year period covered by this dataset?
 */
public class Question3 {
    public static class MainMap extends Mapper<LongWritable, Text, Text, IntWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] split = value.toString().split(",");
            if (split.length != 24)
                return;

            Text reduceKey = new Text(
                split[MainIndex.ORIGIN] + "," +
                    split[MainIndex.YEAR]
            );

            context.write(reduceKey, new IntWritable(1));
        }
    }

    public static class MainReduce extends Reducer<Text, IntWritable, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            long sum = 0;

            for (IntWritable value : values) {
                sum += value.get();
            }

            context.write(key, new Text(String.valueOf(sum)));
        }
    }

    public static class IntermediateMap extends Mapper<LongWritable, Text, Text, Text> {
        // reads intermediate part-r-00000 file produced by MainReduce
        // ORIGIN,YEAR\tCOUNT
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] split = value.toString().split("\\t");
            if (split.length != 2)
                return;

            String[] originAndYear = split[0].split(",");
            if (originAndYear.length != 2)
                return;
            Utils.debug("IntermediateMap: " + value + " : " + split.length);

            Text iata = new Text(originAndYear[0]);
            Text yearAndCount = new Text(originAndYear[1] + "," + split[1]);
            Utils.debug("Intermediate writing: " + iata + " : " + yearAndCount);

            context.write(iata, yearAndCount);
        }
    }

    public static class AirportsMap extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] split = value.toString().split(",");
            if (split.length != 4)
                return;
            Utils.debug("AirportsMap: " + value + " : " + split.length);

            Text iata = new Text(split[AirportsIndex.IATA].replace("\"", ""));
            Text airport = new Text(split[AirportsIndex.AIRPORT].replace("\"", ""));
            Utils.debug("AirportsMap writing: " + iata + " : " + airport);

            context.write(iata, airport);
        }
    }

    public static class FinalReduce extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // ORIGIN\tYEAR,COUNT
            // ORIGIN\tAIRPORT
            String airport = "";
            long count = 0;
            Utils.debug("FinalReduce: " + key);
            for (Text value : values) {
                Utils.debug(value);
                String[] yearAndCount = value.toString().split(",");
                Utils.debug(value + " : " + yearAndCount.length + " : " + Arrays.toString(yearAndCount));
                if (yearAndCount.length == 2 && isInteger(yearAndCount[1])) {
                    count += Integer.parseInt(yearAndCount[1]);
                }
                else {
                    airport = value.toString();
                }
            }

            // todo need to output year too
            if (!airport.isEmpty()) {
                context.write(new Text(airport), new Text(String.valueOf(count)));
            }
        }
    }

    private static boolean isInteger(String string) {
        try {
            Integer.parseInt(string);
            return true;
        }
        catch (Exception e) {
            return false;
        }
    }
}
