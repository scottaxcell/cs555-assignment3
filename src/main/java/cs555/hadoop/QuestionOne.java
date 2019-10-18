package cs555.hadoop;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * What is the best time-of-the-day/day-of-week/time-of-year to fly to minimize delays?
 */
public class QuestionOne {
    public static class TimeOfDayMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] split = value.toString().split(",", -1);
            if (split.length != Constants.MAIN_SPLIT_LENGTH)
                return;

            Text timeOfDay = new Text(split[MainIndex.DEP_TIME].trim());
            LongWritable delay = new LongWritable(sumDelays(split));
            context.write(timeOfDay, delay);
        }
    }

    public static class DayOfWeekMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] split = value.toString().split(",", -1);
            if (split.length != Constants.MAIN_SPLIT_LENGTH)
                return;

            Text dayOfWeek = new Text(split[MainIndex.DAY_OF_WEEK].trim());
            LongWritable delay = new LongWritable(sumDelays(split));
            context.write(dayOfWeek, delay);
        }
    }

    public static class TimeOfYearMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] split = value.toString().split(",", -1);
            if (split.length != Constants.MAIN_SPLIT_LENGTH)
                return;

            Text month = new Text(split[MainIndex.MONTH].trim());
            LongWritable delay = new LongWritable(sumDelays(split));
            context.write(month, delay);
        }
    }

    private static long sumDelays(String[] split) {
        return Utils.parseDelay(split[MainIndex.ARR_DELAY].trim()) +
            Utils.parseDelay(split[MainIndex.DEP_DELAY].trim()) +
            Utils.parseDelay(split[MainIndex.CARRIER_DELAY].trim()) +
            Utils.parseDelay(split[MainIndex.WEATHER_DELAY].trim()) +
            Utils.parseDelay(split[MainIndex.NAS_DELAY].trim()) +
            Utils.parseDelay(split[MainIndex.SECURITY_DELAY].trim()) +
            Utils.parseDelay(split[MainIndex.LATE_AIRCRAFT_DELAY].trim());
    }

    public static class Reducer extends org.apache.hadoop.mapreduce.Reducer<Text, LongWritable, Text, Text> {
        private Map<String, Long> timeToDelay;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            timeToDelay = new HashMap<>();
        }

        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long sum = 0;
            long numValues = 0;
            for (LongWritable value : values) {
                sum += value.get();
                numValues++;
            }
            long averageDelay = sum / numValues;
            timeToDelay.put(String.valueOf(key), averageDelay);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            timeToDelay.entrySet().stream()
                .sorted(Map.Entry.comparingByValue())
                .forEach(e -> {
                    try {
                        context.write(new Text(String.valueOf(e.getValue())), new Text(e.getKey()));
                    }
                    catch (IOException | InterruptedException ex) {
                        ex.printStackTrace();
                    }
                });

            super.cleanup(context);
        }
    }
}
