package com.revature.reduce;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class GrossGraduationSexRatioReducer extends Reducer<Text, DoubleWritable, Text, Text> {
	
	@Override
	public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
		throws IOException, InterruptedException {
		
		double male = -1, female = -1;
		for (DoubleWritable value: values) {
			if (value.get()!=Double.MIN_VALUE) {
				if (value.get()>= 0)
					male = value.get();
				else
					female = value.get()*-1;
			}
		}
		
		if (male!=-1 && female!=-1)
			context.write(key, new Text((male/female)+""));
		else 
			context.write(key, new Text("NaN"));
	}

}
