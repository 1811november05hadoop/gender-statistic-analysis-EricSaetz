package com.revature.reduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AverageReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
	
	@Override
	public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
		throws IOException, InterruptedException {
		
		List<Integer> missingYears = new ArrayList<>();
		double prev = -1;
		double avg = 0.0;
		int count = 0;
		for (DoubleWritable value: values) {
			if (value.get()==Double.MIN_VALUE) {
				missingYears.add(2000+count);
			} else {
				if (prev!=-1) {
					avg+=value.get()-prev;
				}
				prev = value.get();
			}
			count++;
		}
		
		avg/=(double)count;
		
		String stringKey = key.toString();
		stringKey+=": Missing Years: ";
		for (int n:missingYears)
			stringKey+= n+", ";
		key = new Text(stringKey);
		
		context.write(key, new DoubleWritable(avg));
	}

}
