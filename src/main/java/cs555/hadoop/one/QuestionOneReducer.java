package cs555.hadoop.one;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class QuestionOneReducer extends Reducer<Text, LongWritable, Text, Text> {
    Map<String, Long> timeToDelay;

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
