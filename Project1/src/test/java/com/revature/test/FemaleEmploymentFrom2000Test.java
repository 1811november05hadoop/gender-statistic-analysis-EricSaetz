package com.revature.test;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import com.revature.map.FemaleEmploymentFrom2000;
import com.revature.reduce.ChangeInEmploymentReducer;

public class FemaleEmploymentFrom2000Test {
	
	private MapDriver<LongWritable, Text, Text, DoubleWritable> mapDriver;
	private ReduceDriver<Text, DoubleWritable, Text, Text> reduceDriver;
	private MapReduceDriver<LongWritable, Text, Text, DoubleWritable, Text, Text> mapReduceDriver;
	
	@Before
	public void setUp() {
		FemaleEmploymentFrom2000 mapper = new FemaleEmploymentFrom2000();
		mapDriver = new MapDriver<>();
		mapDriver.setMapper(mapper);
		
		ChangeInEmploymentReducer reducer = new ChangeInEmploymentReducer();
		reduceDriver = new ReduceDriver<>();
		reduceDriver.setReducer(reducer);
		
		mapReduceDriver = new MapReduceDriver<>();
		mapReduceDriver.setMapper(mapper);
		mapReduceDriver.setReducer(reducer);
	}
	
	@Test
	public void testFemaleEmploymentFrom2000Mapper() {
		mapDriver.withInput(new LongWritable(1),new Text("\"India\",\"IND\",\"Employment to population ratio, 15+, female (%) "
				+ "(modeled ILO estimate)\",\"SL.EMP.TOTL.SP.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\""
				+ ",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\""
				+ ",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"33.4739990234375\",\"33.7470016479492\""
				+ ",\"34.0029983520508\",\"34.3160018920898\",\"33.9570007324219\",\"33.6539993286133\""
				+ ",\"\",\"\",\"\",\"32.390998840332\""
				+ ",\"\",\"\",\"\",\"\""
				+ ",\"\",\"\",\"\",\"30.3290004730225\""
				+ ",\"\",\"\",\"\",\"\""
				+ ",\"\",\"\",\"\",\"25.8819999694824\","));
		
		mapDriver.withOutput(new Text("India : Employment to population ratio, 15+, female (%) (modeled ILO estimate)"),
				new DoubleWritable(32.390998840332));
		mapDriver.withOutput(new Text("India : Employment to population ratio, 15+, female (%) (modeled ILO estimate)"),
				new DoubleWritable(Double.MIN_VALUE));
		mapDriver.withOutput(new Text("India : Employment to population ratio, 15+, female (%) (modeled ILO estimate)"),
				new DoubleWritable(Double.MIN_VALUE));
		mapDriver.withOutput(new Text("India : Employment to population ratio, 15+, female (%) (modeled ILO estimate)"),
				new DoubleWritable(Double.MIN_VALUE));
		mapDriver.withOutput(new Text("India : Employment to population ratio, 15+, female (%) (modeled ILO estimate)"),
				new DoubleWritable(Double.MIN_VALUE));
		mapDriver.withOutput(new Text("India : Employment to population ratio, 15+, female (%) (modeled ILO estimate)"),
				new DoubleWritable(Double.MIN_VALUE));
		mapDriver.withOutput(new Text("India : Employment to population ratio, 15+, female (%) (modeled ILO estimate)"),
				new DoubleWritable(Double.MIN_VALUE));
		mapDriver.withOutput(new Text("India : Employment to population ratio, 15+, female (%) (modeled ILO estimate)"),
				new DoubleWritable(Double.MIN_VALUE));
		mapDriver.withOutput(new Text("India : Employment to population ratio, 15+, female (%) (modeled ILO estimate)"),
				new DoubleWritable(30.3290004730225));
		mapDriver.withOutput(new Text("India : Employment to population ratio, 15+, female (%) (modeled ILO estimate)"),
				new DoubleWritable(Double.MIN_VALUE));
		mapDriver.withOutput(new Text("India : Employment to population ratio, 15+, female (%) (modeled ILO estimate)"),
				new DoubleWritable(Double.MIN_VALUE));
		mapDriver.withOutput(new Text("India : Employment to population ratio, 15+, female (%) (modeled ILO estimate)"),
				new DoubleWritable(Double.MIN_VALUE));
		mapDriver.withOutput(new Text("India : Employment to population ratio, 15+, female (%) (modeled ILO estimate)"),
				new DoubleWritable(Double.MIN_VALUE));
		mapDriver.withOutput(new Text("India : Employment to population ratio, 15+, female (%) (modeled ILO estimate)"),
				new DoubleWritable(Double.MIN_VALUE));
		mapDriver.withOutput(new Text("India : Employment to population ratio, 15+, female (%) (modeled ILO estimate)"),
				new DoubleWritable(Double.MIN_VALUE));
		mapDriver.withOutput(new Text("India : Employment to population ratio, 15+, female (%) (modeled ILO estimate)"),
				new DoubleWritable(Double.MIN_VALUE));
		mapDriver.withOutput(new Text("India : Employment to population ratio, 15+, female (%) (modeled ILO estimate)"),
				new DoubleWritable(25.8819999694824));
		
		mapDriver.runTest();
	}
	
	@Test
	public void testChangeInEmploymentReducer() {
		List<DoubleWritable> values = new ArrayList<>();
    	values.add(new DoubleWritable(32.390998840332));
    	values.add(new DoubleWritable(25.8819999694824));
    	
    	reduceDriver.withInput(new Text("India : Employment to population ratio, 15+, female (%) (modeled ILO estimate)")
    			, values);
    	reduceDriver.withOutput(new Text("India : Employment to population ratio, 15+, female (%) (modeled ILO estimate): (2000,2001)")
    			, new Text(-6.508998870849602+""));
    	
    	reduceDriver.runTest();
	}
	
	@Test 
	public void testChangeInEmploymentMapReducer() {
		mapReduceDriver.withInput(new LongWritable(1),new Text("\"India\",\"IND\",\"Employment to population ratio, 15+, female (%) "
				+ "(modeled ILO estimate)\",\"SL.EMP.TOTL.SP.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\""
				+ ",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\""
				+ ",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"33.4739990234375\",\"33.7470016479492\""
				+ ",\"34.0029983520508\",\"34.3160018920898\",\"33.9570007324219\",\"33.6539993286133\""
				+ ",\"33.2540016174316\",\"32.765998840332\",\"32.6920013427734\",\"32.390998840332\""
				+ ",\"33.0559997558594\",\"33.4210014343262\",\"34.1599998474121\",\"34.7490005493164\""
				+ ",\"34.9339981079102\",\"33.3160018920898\",\"32.0750007629395\",\"30.3290004730225\""
				+ ",\"28.9529991149902\",\"27.3129997253418\",\"26.4869995117188\",\"25.6909999847412\""
				+ ",\"25.6770000457764\",\"25.6180000305176\",\"25.757999420166\",\"25.8819999694824\","));
		
		mapReduceDriver.withOutput(new Text("India : Employment to population ratio, 15+, female (%) (modeled ILO estimate): (2000,2016)")
    			, new Text(-6.508998870849602+""));
		
		mapReduceDriver.runTest();
	}
}
