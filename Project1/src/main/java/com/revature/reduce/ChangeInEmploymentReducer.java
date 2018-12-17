package com.revature.reduce;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ChangeInEmploymentReducer extends Reducer<Text, DoubleWritable, Text, Text> {
	
	@Override
	public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
		throws IOException, InterruptedException {
		
		double first = -1;
		double last = -1;
		int firstYear=2000, lastYear=1999;
		int count = 0;
		for (DoubleWritable value: values) {
			if (value.get()!=Double.MIN_VALUE) {
				if (count==0) {
					first = value.get();
				}
				else if (first==-1){
					firstYear++;
				}
				last = value.get();
				lastYear++;
				count++;
			}
		}
		
		if (first!=-1 && last!=-1)
			context.write(new Text(key.toString()+": (" + firstYear+","+lastYear+")"), new Text((last-first)+""));
		else
			context.write(new Text(key.toString()+": (" + firstYear+","+lastYear+")"), new Text("NaN"));
	}

}
