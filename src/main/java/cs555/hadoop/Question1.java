package cs555.hadoop;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * What is the best time-of-the-day/day-of-week/time-of-year to fly to minimize delays?
 */
public class Question1 {
    public static class Map extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] split = value.toString().split(",");
            cs555.pastry.util.Utils.debug("Map -- " + value);
            context.write(new Text(split[MainIndex.YEAR]), value);
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            cs555.pastry.util.Utils.debug("Reduce -- " + key);
            for (Text value : values) {
                cs555.pastry.util.Utils.debug("  value: " + value);
            }
        }
    }
}
