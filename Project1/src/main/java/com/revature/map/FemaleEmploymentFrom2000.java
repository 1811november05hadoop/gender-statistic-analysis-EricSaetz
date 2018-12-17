package com.revature.map;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Maps Female Employment Rate of each country using the earliest recorded data point(minimum 2000)
 * and the latest recorded data point. 
 *  
 * This code assumes that the "Employment to population ratio" is the relevant data category to
 * answer this question, and that the input data will be in csv format. If the data is missing
 * for a year it passes the minimum Double value.
 */

public class FemaleEmploymentFrom2000 extends Mapper<LongWritable, Text, Text, DoubleWritable> {
	
	@Override
	public void map(LongWritable key, Text value, Context context)
		throws IOException, InterruptedException {
			String category, country;
			List<DoubleWritable> statistics = new ArrayList<>();
			String raw_data = value.toString().replace("\t", " ");
			String data[] = raw_data.substring(1,raw_data.length()-2).split("\",\"",-1);
			
			if (data.length>=60) {
				country = data[0];
				category = data[2];
				
					if (category.contains("Employment to population ratio") && category.contains("female (%)")) {
						for (int i=44;i<=60;i++) {
							if (!data[i].equals("")) {
								statistics.add(new DoubleWritable(Double.parseDouble(data[i])));
							}
							else {
								statistics.add(null);
							}
						}
						
						for (DoubleWritable val:statistics) {
							if (val!=null) {
								context.write(new Text(country + " : " + category), val);
							} else {
								context.write(new Text(country + " : " + category),new DoubleWritable(Double.MIN_VALUE));
							}
						}
						
					}
			}
	}
}
