package cs555.hadoop.one;

import cs555.hadoop.Constants;
import cs555.hadoop.MainIndex;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * What is the best time-of-the-day/day-of-week/time-of-year to fly to minimize delays?
 */
public class QuestionOneMapper {
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

    private static long parseDelay(String string) {
        try {
            float delay = Float.parseFloat(string);
            return delay < 0 ? 0 : (long) delay;
        }
        catch (Exception e) {
            return 0;
        }
    }

    private static long sumDelays(String[] split) {
        return parseDelay(split[MainIndex.ARR_DELAY].trim()) +
            parseDelay(split[MainIndex.DEP_DELAY].trim()) +
            parseDelay(split[MainIndex.CARRIER_DELAY].trim()) +
            parseDelay(split[MainIndex.WEATHER_DELAY].trim()) +
            parseDelay(split[MainIndex.NAS_DELAY].trim()) +
            parseDelay(split[MainIndex.SECURITY_DELAY].trim()) +
            parseDelay(split[MainIndex.LATE_AIRCRAFT_DELAY].trim());
    }
}
