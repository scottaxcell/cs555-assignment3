package cs555.hadoop;

import cs555.hadoop.one.QuestionOneMapper;
import cs555.hadoop.one.QuestionOneReducer;
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
        jobRunner.runQuestionOne();
//        jobRunner.runQuestionThree();
    }

    private void runQuestionOne() {
        try {
            Job timeOfDayJob = Job.getInstance(configuration, "Question 1 TOD");

            timeOfDayJob.setJarByClass(QuestionOneMapper.class);

            timeOfDayJob.setMapperClass(QuestionOneMapper.TimeOfDayMapper.class);
            timeOfDayJob.setMapOutputKeyClass(Text.class);
            timeOfDayJob.setMapOutputValueClass(LongWritable.class);

            timeOfDayJob.setReducerClass(QuestionOneReducer.class);
            timeOfDayJob.setOutputKeyClass(Text.class);
            timeOfDayJob.setOutputValueClass(Text.class);

            FileInputFormat.addInputPath(timeOfDayJob, DATA_MAIN_PATH);
            FileOutputFormat.setOutputPath(timeOfDayJob, new Path("/home/question1_tod"));

            timeOfDayJob.waitForCompletion(false);

            // ----------

            Job timeOfWeekJob = Job.getInstance(configuration, "Question 1 DOW");

            timeOfWeekJob.setJarByClass(QuestionOneMapper.class);

            timeOfWeekJob.setMapperClass(QuestionOneMapper.DayOfWeekMapper.class);
            timeOfWeekJob.setMapOutputKeyClass(Text.class);
            timeOfWeekJob.setMapOutputValueClass(LongWritable.class);

            timeOfWeekJob.setReducerClass(QuestionOneReducer.class);
            timeOfWeekJob.setOutputKeyClass(Text.class);
            timeOfWeekJob.setOutputValueClass(Text.class);

            FileInputFormat.addInputPath(timeOfWeekJob, DATA_MAIN_PATH);
            FileOutputFormat.setOutputPath(timeOfWeekJob, new Path("/home/question1_dow"));

            timeOfWeekJob.waitForCompletion(false);

            // ----------

            Job timeOfYearJob = Job.getInstance(configuration, "Question 1 TOY");

            timeOfYearJob.setJarByClass(QuestionOneMapper.class);

            timeOfYearJob.setMapperClass(QuestionOneMapper.TimeOfYearMapper.class);
            timeOfYearJob.setMapOutputKeyClass(Text.class);
            timeOfYearJob.setMapOutputValueClass(LongWritable.class);

            timeOfYearJob.setReducerClass(QuestionOneReducer.class);
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

            job.setJarByClass(Question3.class);

            job.setMapperClass(Question3.MainMap.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);

            job.setReducerClass(Question3.MainReduce.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            FileInputFormat.addInputPath(job, DATA_MAIN_PATH);
            FileOutputFormat.setOutputPath(job, new Path("/home/question3part1"));

            job.waitForCompletion(false);

            // ----------

            Job mapIataToAirportJob = Job.getInstance(configuration, "Question 3 Part 2");

            mapIataToAirportJob.setJarByClass(Question3.class);

            mapIataToAirportJob.setMapperClass(Question3.IntermediateMap.class);
            mapIataToAirportJob.setMapperClass(Question3.AirportsMap.class);
            mapIataToAirportJob.setMapOutputKeyClass(Text.class);
            mapIataToAirportJob.setMapOutputValueClass(Text.class);

            mapIataToAirportJob.setReducerClass(Question3.FinalReduce.class);
            mapIataToAirportJob.setOutputKeyClass(Text.class);
            mapIataToAirportJob.setOutputValueClass(Text.class);

            MultipleInputs.addInputPath(mapIataToAirportJob, new Path("/home/question3part1/" + PART_R_00000), TextInputFormat.class, Question3.IntermediateMap.class);
            MultipleInputs.addInputPath(mapIataToAirportJob, DATA_SUPPLEMENTARY_AIRPORTS_CSV_PATH, TextInputFormat.class, Question3.AirportsMap.class);
            FileOutputFormat.setOutputPath(mapIataToAirportJob, new Path("/home/question3part2"));

            mapIataToAirportJob.waitForCompletion(false);
        }
        catch (IOException | InterruptedException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}
