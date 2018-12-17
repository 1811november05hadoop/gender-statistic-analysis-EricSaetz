package com.revature;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.revature.map.WomenGraduationLessThanThirtyMapper;

public class WomenGraduationLessThanThirtyDriver {
	public static void main(String args[]) throws Exception {
		if (args.length != 2) {
			System.out.println("Incorrect amount of args!");
			System.exit(-1);
		}
		
		Job job = new Job();
		
		job.setJarByClass(WomenGraduationLessThanThirtyDriver.class);
		
		job.setJobName("⦁Identify the countries where % of female graduates is less than 30%. ");
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job,new Path(args[1]));
		
		job.setMapperClass(WomenGraduationLessThanThirtyMapper.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		boolean success = job.waitForCompletion(true);
		System.exit(success ? 0 : 1);
	}
}
