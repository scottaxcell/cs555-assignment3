package cs555.hadoop;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * What is the best time-of-the-day/day-of-week/time-of-year to fly to minimize delays?
 */
public class Question1 {
    public static class Map extends Mapper<LongWritable, Text, Text, LongWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] split = value.toString().split(",");
            cs555.pastry.util.Utils.debug("split.length: " + split.length);
            if (split.length != 24)
                return;
            cs555.pastry.util.Utils.debug(value);

            Text reduceKey = new Text(
                split[MainIndex.DEP_TIME] + "," +
                    split[MainIndex.DAY_OF_WEEK] + "," +
                    split[MainIndex.MONTH]);

            cs555.pastry.util.Utils.debug("reduceKey: " + reduceKey);

            LongWritable delay = new LongWritable(
                parseDelay(split[MainIndex.ARR_DELAY]) +
                    parseDelay(split[MainIndex.DEP_DELAY]));

            cs555.pastry.util.Utils.debug("delay: " + delay);

            context.write(reduceKey, delay);
        }
    }

    public static class Reduce extends Reducer<Text, LongWritable, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long delay = 0;

            for (LongWritable value : values) {
                delay += value.get();
            }

            context.write(new Text(String.valueOf(delay)), key);
        }
    }

    static long parseDelay(String string) {
        try {
            float delay = Float.parseFloat(string);
            return delay < 0 ? 0 : (long) delay;
        }
        catch (Exception e) {
            return 0;
        }
    }
}
