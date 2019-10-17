package cs555.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class JobRunner {
    private static final Path DATA_MAIN_PATH = new Path("/data/main");
    private static final Path DATA_SUPPLEMENTARY_PATH = new Path("/data/supplementary");

    Configuration configuration = new Configuration();

    public static void main(String[] args) {
        JobRunner jobRunner = new JobRunner();
        jobRunner.runQuestionOne();
    }

    private void runQuestionOne() {
        try {
            Job job = Job.getInstance(configuration, "Question 1");

            job.setJarByClass(Question1.class);

            job.setMapperClass(Question1.Map.class);
            job.setCombinerClass(Question1.Reduce.class);
            job.setReducerClass(Question1.Reduce.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            FileInputFormat.addInputPath(job, DATA_MAIN_PATH);
            FileOutputFormat.setOutputPath(job, new Path("/home/question1"));

            job.waitForCompletion(true);
        }
        catch (IOException | InterruptedException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}
