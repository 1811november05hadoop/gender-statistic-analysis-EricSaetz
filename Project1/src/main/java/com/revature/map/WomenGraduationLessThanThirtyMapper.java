package com.revature.map;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 
 */

public class WomenGraduationLessThanThirtyMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	@Override
	public void map(LongWritable key, Text value, Context context)
		throws IOException, InterruptedException {
			String category, country;
			Double statistic;
			String raw_data = value.toString().replace("\t", " ");
			String data[] = raw_data.substring(1,raw_data.length()-2).split("\",\"",-1);
			
			if (data.length>=60) {
				country = data[0];
				category = data[2];
				
				if (category.contains("Gross graduation ratio") && category.contains("female (%)")) { 
					for (int i = 59;i<=59;i++) {
						if (!data[i].equals(""))
							statistic = Double.parseDouble(data[i]);
						else
							statistic = Double.MIN_VALUE;
					
						if (statistic<30.0) {
							if (statistic!=Double.MIN_VALUE)
								context.write(new Text(country + " : " + category + " : " + (1956+i)), new Text(statistic+""));
							else
								context.write(new Text(country + " : " + category + " : " + (1956+i)), new Text("NaN"));
						}
					}
				}
			}
	}
}
