package com.revature;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.revature.map.GrossGraduationSexRatioMapper;
import com.revature.reduce.GrossGraduationSexRatioReducer;

public class GrossGraduationSexRatioDriver {
	public static void main(String args[]) throws Exception {
		if (args.length != 2) {
			System.out.println("Incorrect amount of args!");
			System.exit(-1);
		}
		
		Job job = new Job();
		
		job.setJarByClass(GrossGraduationSexRatioDriver.class);
		
		job.setJobName("⦁	Additionally, based on your data exploration and analysis, evaluate one business factor that "
				+ "you consider important, and make this your own requirement.\n");
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job,new Path(args[1]));
		
		job.setMapperClass(GrossGraduationSexRatioMapper.class);
		job.setReducerClass(GrossGraduationSexRatioReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		
		boolean success = job.waitForCompletion(true);
		System.exit(success ? 0 : 1);
	}
}
