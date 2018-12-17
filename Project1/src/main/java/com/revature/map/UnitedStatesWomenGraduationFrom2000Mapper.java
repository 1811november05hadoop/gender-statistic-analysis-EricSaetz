package com.revature.map;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Maps the Graduation Rate for women in the United States since the
 * year 2000 to the relevant category and year.
 * 
 * This Mapper assumes that the Gross Graduation Rate is the relevant field to
 * answer this question, and that the "United States" is the correctly marked country.
 * If there is data missing for a given year, it passes the minimum Double value.
 */

public class UnitedStatesWomenGraduationFrom2000Mapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
	
	@Override
	public void map(LongWritable key, Text value, Context context)
		throws IOException, InterruptedException {
			String category, country;
			List<DoubleWritable> statistics = new ArrayList<>();
			String raw_data = value.toString().replace("\t", " ");
			String data[] = raw_data.substring(1,raw_data.length()-2).split("\",\"",-1);
			
			country = data[0];
			category = data[2];
			
			if (country.equals("United States") && category.contains("Gross graduation ratio") && category.contains("female (%)")) {
				for (int i=44;i<60;i++) {
					if (!data[i].equals("")) {
						statistics.add(new DoubleWritable(Double.parseDouble(data[i])));
					} else {
						statistics.add(new DoubleWritable(Double.MIN_VALUE));
					}
				}
				
				for (DoubleWritable statistic:statistics)
					context.write(new Text(country + " : " + category), statistic);
			}
	}
}
