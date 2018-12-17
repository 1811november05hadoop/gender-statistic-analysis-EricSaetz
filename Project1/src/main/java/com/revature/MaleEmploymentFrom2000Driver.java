package com.revature;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.revature.map.MaleEmploymentFrom2000;
import com.revature.reduce.ChangeInEmploymentReducer;

public class MaleEmploymentFrom2000Driver {
	public static void main(String args[]) throws Exception {
		if (args.length != 2) {
			System.out.println("Incorrect amount of args!");
			System.exit(-1);
		}
		
		Job job = new Job();
		
		job.setJarByClass(MaleEmploymentFrom2000Driver.class);
		
		job.setJobName("⦁List the % of change in male employment from the year 2000.");
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job,new Path(args[1]));
		
		job.setMapperClass(MaleEmploymentFrom2000.class);
		job.setReducerClass(ChangeInEmploymentReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		
		boolean success = job.waitForCompletion(true);
		System.exit(success ? 0 : 1);
	}
}
