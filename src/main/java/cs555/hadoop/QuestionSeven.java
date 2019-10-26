package cs555.hadoop;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * What are the aspects that you can infer from the LateAircraftDelay field? Develop a program that
 * harnesses this field.
 *
 * What are the best and worst time-of-the-day to travel to avoid late aircraft delays?
 */
public class QuestionSeven {
    public static class MainMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] split = value.toString().split(",", -1);
            if (split.length != Constants.MAIN_SPLIT_LENGTH)
                return;

            Text timeOfDay = new Text(split[MainIndex.DEP_TIME].trim());
            LongWritable delay = new LongWritable(Utils.parseDelay(split[MainIndex.LATE_AIRCRAFT_DELAY].trim()));
            context.write(timeOfDay, delay);
        }
    }

    public static class Reducer extends org.apache.hadoop.mapreduce.Reducer<Text, LongWritable, Text, Text> {
        private Map<String, Long> timeToAvgLateAircraftDelay;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            timeToAvgLateAircraftDelay = new HashMap<>();
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
            timeToAvgLateAircraftDelay.put(String.valueOf(key), averageDelay);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            timeToAvgLateAircraftDelay.entrySet().stream()
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
