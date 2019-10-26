package cs555.hadoop;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

/**
 * Which carriers have the most delays? You should report on the total number of delayed flights
 * and also the total number of minutes that were lost to delays. Which carrier has the highest average
 * delay?
 */
public class QuestionFive {

    public static class MainMapper extends org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] split = value.toString().split(",", -1);
            if (split.length != Constants.MAIN_SPLIT_LENGTH)
                return;

            Text code = new Text(split[MainIndex.UNIQUE_CARRIER].trim());
            int delay = Utils.parseDelay(split[MainIndex.CARRIER_DELAY].trim());
            if (delay > 0) {
                Utils.debug("main: " + code + " -- " + delay);
                context.write(code, new Text(Constants.MAIN_MAP_ID + delay));
            }
        }
    }

    public static class CarriersMapper extends org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] split = value.toString().split(",", -1);
            if (split.length != Constants.CARRIERS_SPLIT_LENGTH)
                return;

            Text code = new Text(split[CarriersIndex.CODE].replace("\"", "").trim());
            Text description = new Text(split[CarriersIndex.DESCRIPTION].replace("\"", "").trim());
            Utils.debug("carriers: " + code + " -- " + description);
            context.write(code, description);
        }
    }

    public static class Reducer extends org.apache.hadoop.mapreduce.Reducer<Text, Text, Text, Text> {
        private Map<String, TotalAndCount> codeToDelays;
        private Map<String, String> codeToDescription;

        class TotalAndCount implements Comparable {
            long total = 0;
            int count = 0;

            @Override
            public int compareTo(Object o) {
                return Integer.compare(count, ((TotalAndCount) o).count);
            }
        }

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            codeToDelays = new HashMap<>();
            codeToDescription = new HashMap<>();
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value : values) {
                Utils.debug("reducer: " + key + " -- " + value);
                if (value.toString().startsWith(Constants.MAIN_MAP_ID)) {
                    String delayStr = value.toString().replaceAll(Constants.MAIN_MAP_ID, "");
                    TotalAndCount tac = codeToDelays.computeIfAbsent(key.toString(), k -> new TotalAndCount());
                    tac.count++;
                    tac.total += Integer.parseInt(delayStr);
                }
                else {
                    codeToDescription.putIfAbsent(key.toString(), value.toString());
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            codeToDelays.entrySet().stream()
                .sorted((e1, e2) -> e1.getValue().compareTo(e2.getValue()))
                .map(e -> String.format("%d (%d)\t(%s) %s", e.getValue().count, e.getValue().total, e.getKey(), codeToDescription.get(e.getKey())))
                .forEach(s -> {
                    try {
                        context.write(new Text(s), new Text());
                    }
                    catch (IOException | InterruptedException ex) {
                        ex.printStackTrace();
                    }
                });

            Map<String, Double> carrierToAvgDelay = new HashMap<>();
            for (Map.Entry<String, TotalAndCount> entry : codeToDelays.entrySet()) {
                TotalAndCount tac = entry.getValue();
                carrierToAvgDelay.put(entry.getKey(), (double) (tac.total / tac.count));
            }

            carrierToAvgDelay.entrySet().stream()
                .max(Comparator.comparingDouble(Map.Entry::getValue))
                .map(e -> String.format("%.2f (%s) %s", e.getValue(), e.getKey(), codeToDescription.get(e.getKey())))
                .ifPresent(s -> {
                    try {
                        context.write(new Text(), new Text());
                        context.write(new Text("Carrier with highest average delay: " + s), new Text());
                    }
                    catch (IOException | InterruptedException ex) {
                        ex.printStackTrace();
                    }
                });

            super.cleanup(context);
        }
    }
}
