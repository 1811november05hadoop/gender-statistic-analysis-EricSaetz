package com.revature.map;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Maps Tertiary Gross Graduation Sex Ratio for each year after and including 1999.
 * This assumes that the Gross Graduation Ratio is the relevant data field to answer
 * this question. If the relevant data field is empty it is passed with the minimum
 * Double value;
 */

public class GrossGraduationSexRatioMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
	
	@Override
	public void map(LongWritable key, Text value, Context context)
		throws IOException, InterruptedException {
			String category, country;
			Double statistic;
			String raw_data = value.toString().replace("\t", " ");
			String data[] = raw_data.substring(1,raw_data.length()-2).split("\",\"",-1);
			country = data[0];
			category = data[2];
				
				
			if (category.contains("Gross graduation ratio, tertiary,")) {
				for (int i=44;i<=60;i++) {
					if (!data[i].equals("")) {
						statistic = Double.parseDouble(data[i]);
						if (category.contains(" female (%)"))
							statistic *= -1;
					}
					else {
						statistic = Double.MIN_VALUE;
					}
					
					context.write(new Text(country + " : " + "Gross graduation, tertiary, sex ratio (m/f) : " + (1956+i)), new DoubleWritable(statistic));
				}
			}
	}
}
