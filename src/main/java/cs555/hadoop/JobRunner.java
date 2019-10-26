package cs555.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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
    private static final Path DATA_SUPPLEMENTARY_CARRIERS_CSV_PATH = new Path("/data/supplementary/carriers.csv");

    Configuration configuration = new Configuration();

    public static void main(String[] args) {
        JobRunner jobRunner = new JobRunner();
//        jobRunner.runQuestionOne();
//        jobRunner.runQuestionThree();
//        jobRunner.runQuestionFour();
//        jobRunner.runQuestionFive();
//        jobRunner.runQuestionSix();
        jobRunner.runQuestionSeven();
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
            Job busiestAirportsJob = Job.getInstance(configuration, "Question 3");

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
            FileOutputFormat.setOutputPath(busiestAirportsJob, new Path("/home/question3"));

            busiestAirportsJob.waitForCompletion(false);
        }
        catch (IOException | InterruptedException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    private void runQuestionFour() {
        try {
            Job job = Job.getInstance(configuration, "Question 4");

            job.setJarByClass(QuestionFour.class);

            job.setMapperClass(QuestionFour.MainMapper.class);
            job.setMapperClass(QuestionFour.AirportsMapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);

            job.setReducerClass(QuestionFour.Reducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            MultipleInputs.addInputPath(job, DATA_MAIN_PATH, TextInputFormat.class, QuestionFour.MainMapper.class);
            MultipleInputs.addInputPath(job, DATA_SUPPLEMENTARY_AIRPORTS_CSV_PATH, TextInputFormat.class, QuestionFour.AirportsMapper.class);
            FileOutputFormat.setOutputPath(job, new Path("/home/question4"));

            job.waitForCompletion(false);
        }
        catch (IOException | InterruptedException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    private void runQuestionFive() {
        try {
            Job job = Job.getInstance(configuration, "Question 5");

            job.setJarByClass(QuestionFive.class);

            job.setMapperClass(QuestionFive.MainMapper.class);
            job.setMapperClass(QuestionFive.CarriersMapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);

            job.setReducerClass(QuestionFive.Reducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            MultipleInputs.addInputPath(job, DATA_MAIN_PATH, TextInputFormat.class, QuestionFive.MainMapper.class);
            MultipleInputs.addInputPath(job, DATA_SUPPLEMENTARY_CARRIERS_CSV_PATH, TextInputFormat.class, QuestionFive.CarriersMapper.class);
            FileOutputFormat.setOutputPath(job, new Path("/home/question5"));

            job.waitForCompletion(false);
        }
        catch (IOException | InterruptedException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    private void runQuestionSix() {
        try {
            Job job = Job.getInstance(configuration, "Question 6");

            job.setJarByClass(QuestionSix.class);

            job.setMapperClass(QuestionSix.MainMapper.class);
            job.setMapperClass(QuestionSix.AirportsMapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);

            job.setReducerClass(QuestionSix.Reducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            MultipleInputs.addInputPath(job, DATA_MAIN_PATH, TextInputFormat.class, QuestionSix.MainMapper.class);
            MultipleInputs.addInputPath(job, DATA_SUPPLEMENTARY_AIRPORTS_CSV_PATH, TextInputFormat.class, QuestionSix.AirportsMapper.class);
            FileOutputFormat.setOutputPath(job, new Path("/home/question6"));

            job.waitForCompletion(false);
        }
        catch (IOException | InterruptedException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    private void runQuestionSeven() {
        try {
            Job timeOfDayJob = Job.getInstance(configuration, "Question 7");

            timeOfDayJob.setJarByClass(QuestionSeven.class);

            timeOfDayJob.setMapperClass(QuestionSeven.MainMapper.class);
            timeOfDayJob.setMapOutputKeyClass(Text.class);
            timeOfDayJob.setMapOutputValueClass(LongWritable.class);

            timeOfDayJob.setReducerClass(QuestionSeven.Reducer.class);
            timeOfDayJob.setOutputKeyClass(Text.class);
            timeOfDayJob.setOutputValueClass(Text.class);

            FileInputFormat.addInputPath(timeOfDayJob, DATA_MAIN_PATH);
            FileOutputFormat.setOutputPath(timeOfDayJob, new Path("/home/question7"));

            timeOfDayJob.waitForCompletion(false);
        }
        catch (IOException | InterruptedException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}
