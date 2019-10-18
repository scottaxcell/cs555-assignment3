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
            Job job = Job.getInstance(configuration, "Question 3 Part 1");

            job.setJarByClass(QuestionThree.class);

            job.setMapperClass(QuestionThree.MainMap.class);
            job.setMapperClass(QuestionThree.AirportsMap.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);

            job.setReducerClass(QuestionThree.Reducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            MultipleInputs.addInputPath(job, DATA_MAIN_PATH, TextInputFormat.class, QuestionThree.MainMap.class);
            MultipleInputs.addInputPath(job, DATA_SUPPLEMENTARY_AIRPORTS_CSV_PATH, TextInputFormat.class, QuestionThree.AirportsMap.class);
            FileOutputFormat.setOutputPath(job, new Path("/home/question3p1"));

            job.waitForCompletion(false);

            // ----------
//
//            Job mapIataToAirportJob = Job.getInstance(configuration, "Question 3 Part 2");
//
//            mapIataToAirportJob.setJarByClass(QuestionThree.class);
//
//            mapIataToAirportJob.setMapperClass(QuestionThree.IntermediateMap.class);
//            mapIataToAirportJob.setMapperClass(QuestionThree.AirportsMap.class);
//            mapIataToAirportJob.setMapOutputKeyClass(Text.class);
//            mapIataToAirportJob.setMapOutputValueClass(Text.class);
//
//            mapIataToAirportJob.setReducerClass(QuestionThree.FinalReduce.class);
//            mapIataToAirportJob.setOutputKeyClass(Text.class);
//            mapIataToAirportJob.setOutputValueClass(Text.class);
//
//            MultipleInputs.addInputPath(mapIataToAirportJob, new Path("/home/question3part1/" + PART_R_00000), TextInputFormat.class, QuestionThree.IntermediateMap.class);
//            MultipleInputs.addInputPath(mapIataToAirportJob, DATA_SUPPLEMENTARY_AIRPORTS_CSV_PATH, TextInputFormat.class, QuestionThree.AirportsMap.class);
//            FileOutputFormat.setOutputPath(mapIataToAirportJob, new Path("/home/question3part2"));
//
//            mapIataToAirportJob.waitForCompletion(false);
        }
        catch (IOException | InterruptedException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}
