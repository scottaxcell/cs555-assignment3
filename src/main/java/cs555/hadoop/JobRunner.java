package cs555.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class JobRunner {
    private static final Path DATA_MAIN_PATH = new Path("/data/main");
    private static final Path DATA_SUPPLEMENTARY_AIRPORTS_CSV_PATH = new Path("/data/supplementary/airports.csv");
    private static final String PART_R_00000 = "/part-r-00000";

    Configuration configuration = new Configuration();

    public static void main(String[] args) {
        JobRunner jobRunner = new JobRunner();
//        jobRunner.runQuestionOne();
        jobRunner.runQuestionThree();
    }

    private void runQuestionOne() {
        try {
            Job timeOfDayJob = Job.getInstance(configuration, "Question 1 TOD");

            timeOfDayJob.setJarByClass(QuestionOne.class);

            timeOfDayJob.setMapperClass(QuestionOne.TimeOfDayMapper.class);
            timeOfDayJob.setMapOutputKeyClass(Text.class);
            timeOfDayJob.setMapOutputValueClass(LongWritable.class);

            timeOfDayJob.setReducerClass(QuestionOne.Reducer.class);
            timeOfDayJob.setOutputKeyClass(Text.class);
            timeOfDayJob.setOutputValueClass(Text.class);

            FileInputFormat.addInputPath(timeOfDayJob, DATA_MAIN_PATH);
            FileOutputFormat.setOutputPath(timeOfDayJob, new Path("/home/question1_tod"));

            timeOfDayJob.waitForCompletion(false);

            // ----------

            Job timeOfWeekJob = Job.getInstance(configuration, "Question 1 DOW");

            timeOfWeekJob.setJarByClass(QuestionOne.class);

            timeOfWeekJob.setMapperClass(QuestionOne.DayOfWeekMapper.class);
            timeOfWeekJob.setMapOutputKeyClass(Text.class);
            timeOfWeekJob.setMapOutputValueClass(LongWritable.class);

            timeOfWeekJob.setReducerClass(QuestionOne.Reducer.class);
            timeOfWeekJob.setOutputKeyClass(Text.class);
            timeOfWeekJob.setOutputValueClass(Text.class);

            FileInputFormat.addInputPath(timeOfWeekJob, DATA_MAIN_PATH);
            FileOutputFormat.setOutputPath(timeOfWeekJob, new Path("/home/question1_dow"));

            timeOfWeekJob.waitForCompletion(false);

            // ----------

            Job timeOfYearJob = Job.getInstance(configuration, "Question 1 TOY");

            timeOfYearJob.setJarByClass(QuestionOne.class);

            timeOfYearJob.setMapperClass(QuestionOne.TimeOfYearMapper.class);
            timeOfYearJob.setMapOutputKeyClass(Text.class);
            timeOfYearJob.setMapOutputValueClass(LongWritable.class);

            timeOfYearJob.setReducerClass(QuestionOne.Reducer.class);
            timeOfYearJob.setOutputKeyClass(Text.class);
            timeOfYearJob.setOutputValueClass(Text.class);

            FileInputFormat.addInputPath(timeOfYearJob, DATA_MAIN_PATH);
            FileOutputFormat.setOutputPath(timeOfYearJob, new Path("/home/question1_toy"));

            timeOfYearJob.waitForCompletion(false);
        }
        catch (IOException | InterruptedException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    private void runQuestionThree() {
        try {
            Job busiestAirportsJob = Job.getInstance(configuration, "Question 3 Part 1");

            busiestAirportsJob.setJarByClass(QuestionThree.class);

            busiestAirportsJob.setMapperClass(QuestionThree.MainMapper.class);
            busiestAirportsJob.setMapperClass(QuestionThree.AirportsMapper.class);
            busiestAirportsJob.setMapOutputKeyClass(Text.class);
            busiestAirportsJob.setMapOutputValueClass(Text.class);

            busiestAirportsJob.setReducerClass(QuestionThree.Reducer.class);
            busiestAirportsJob.setOutputKeyClass(Text.class);
            busiestAirportsJob.setOutputValueClass(Text.class);

            MultipleInputs.addInputPath(busiestAirportsJob, DATA_MAIN_PATH, TextInputFormat.class, QuestionThree.MainMapper.class);
            MultipleInputs.addInputPath(busiestAirportsJob, DATA_SUPPLEMENTARY_AIRPORTS_CSV_PATH, TextInputFormat.class, QuestionThree.AirportsMapper.class);
            FileOutputFormat.setOutputPath(busiestAirportsJob, new Path("/home/question3p1"));

            busiestAirportsJob.waitForCompletion(false);

            // ----------

            Job changesPerYearJob = Job.getInstance(configuration, "Question 3 Part 2");

            changesPerYearJob.setJarByClass(QuestionThree.class);

            changesPerYearJob.setMapperClass(QuestionThree.YearMapper.class);
            changesPerYearJob.setMapOutputKeyClass(IntWritable.class);
            changesPerYearJob.setMapOutputValueClass(IntWritable.class);

            changesPerYearJob.setReducerClass(QuestionThree.YearReducer.class);
            changesPerYearJob.setOutputKeyClass(Text.class);
            changesPerYearJob.setOutputValueClass(Text.class);

            FileInputFormat.addInputPath(changesPerYearJob, DATA_MAIN_PATH);
            FileOutputFormat.setOutputPath(changesPerYearJob, new Path("/home/question3p2"));

            changesPerYearJob.waitForCompletion(false);
        }
        catch (IOException | InterruptedException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}
