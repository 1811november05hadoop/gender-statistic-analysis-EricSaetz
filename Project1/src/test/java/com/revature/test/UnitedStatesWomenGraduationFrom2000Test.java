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

import com.revature.map.UnitedStatesWomenGraduationFrom2000Mapper;
import com.revature.reduce.AverageReducer;

public class UnitedStatesWomenGraduationFrom2000Test {
	
	private MapDriver<LongWritable, Text, Text, DoubleWritable> mapDriver;
	private ReduceDriver<Text, DoubleWritable, Text, DoubleWritable> reduceDriver;
	private MapReduceDriver<LongWritable, Text, Text, DoubleWritable, Text, DoubleWritable> mapReduceDriver;
	
	@Before
	public void setUp() {
		UnitedStatesWomenGraduationFrom2000Mapper mapper = new UnitedStatesWomenGraduationFrom2000Mapper();
		mapDriver = new MapDriver<>();
		mapDriver.setMapper(mapper);
		
		AverageReducer reducer = new AverageReducer();
		reduceDriver = new ReduceDriver<>();
		reduceDriver.setReducer(reducer);
		
		mapReduceDriver = new MapReduceDriver<>();
		mapReduceDriver.setMapper(mapper);
		mapReduceDriver.setReducer(reducer);
	}
	
	@Test
	public void testFemaleEmploymentFrom2000Mapper() {
		mapDriver.withInput(new LongWritable(1),new Text("\"United States\",\"USA\",\"Gross graduation ratio,"
				+ " tertiary, female (%)\",\"SE.TER.CMPL.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"35.85857\",\"37.8298\","
				+ "\"37.43131\",\"38.22037\",\"39.18913\",\"39.84185\",\"40.23865\",\"41.26198\",\"42.00725\","
				+ "\"42.78946\",\"43.68347\",\"\",\"46.37914\",\"47.68032\",\"\",\"\",\"\",\"\","));
		
		mapDriver.withOutput(new Text("United States : Gross graduation ratio, tertiary, female (%)"),
				new DoubleWritable(37.8298));
		mapDriver.withOutput(new Text("United States : Gross graduation ratio, tertiary, female (%)"),
				new DoubleWritable(37.43131));
		mapDriver.withOutput(new Text("United States : Gross graduation ratio, tertiary, female (%)"),
				new DoubleWritable(38.22037));
		mapDriver.withOutput(new Text("United States : Gross graduation ratio, tertiary, female (%)"),
				new DoubleWritable(39.18913));
		mapDriver.withOutput(new Text("United States : Gross graduation ratio, tertiary, female (%)"),
				new DoubleWritable(39.84185));
		mapDriver.withOutput(new Text("United States : Gross graduation ratio, tertiary, female (%)"),
				new DoubleWritable(40.23865));
		mapDriver.withOutput(new Text("United States : Gross graduation ratio, tertiary, female (%)"),
				new DoubleWritable(41.26198));
		mapDriver.withOutput(new Text("United States : Gross graduation ratio, tertiary, female (%)"),
				new DoubleWritable(42.00725));
		mapDriver.withOutput(new Text("United States : Gross graduation ratio, tertiary, female (%)"),
				new DoubleWritable(42.78946));
		mapDriver.withOutput(new Text("United States : Gross graduation ratio, tertiary, female (%)"),
				new DoubleWritable(43.68347));
		mapDriver.withOutput(new Text("United States : Gross graduation ratio, tertiary, female (%)"),
				new DoubleWritable(Double.MIN_VALUE));
		mapDriver.withOutput(new Text("United States : Gross graduation ratio, tertiary, female (%)"),
				new DoubleWritable(46.37914));
		mapDriver.withOutput(new Text("United States : Gross graduation ratio, tertiary, female (%)"),
				new DoubleWritable(47.68032));
		mapDriver.withOutput(new Text("United States : Gross graduation ratio, tertiary, female (%)"),
				new DoubleWritable(Double.MIN_VALUE));
		mapDriver.withOutput(new Text("United States : Gross graduation ratio, tertiary, female (%)"),
				new DoubleWritable(Double.MIN_VALUE));
		mapDriver.withOutput(new Text("United States : Gross graduation ratio, tertiary, female (%)"),
				new DoubleWritable(Double.MIN_VALUE));
		mapDriver.runTest();
	}
	
	@Test
	public void testChangeInEmploymentReducer() {
		List<DoubleWritable> values = new ArrayList<>();
    	values.add(new DoubleWritable(10.0));
    	values.add(new DoubleWritable(Double.MIN_VALUE));
    	values.add(new DoubleWritable(20.0));
    	
    	reduceDriver.withInput(new Text("United States : Gross graduation ratio, tertiary, female (%)")
    			, values);
    	reduceDriver.withOutput(new Text("United States : Gross graduation ratio, tertiary, female (%):"
    			+ " Missing Years: 2001, ")
    			, new DoubleWritable(10.0/3.0));
    	
    	reduceDriver.runTest();
	}
	
	@Test 
	public void testChangeInEmploymentMapReducer() {
		mapReduceDriver.withInput(new LongWritable(1),new Text("\"United States\",\"USA\",\"Gross graduation ratio,"
				+ " tertiary, female (%)\",\"SE.TER.CMPL.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"35.85857\",\"37.8298\","
				+ "\"37.43131\",\"38.22037\",\"39.18913\",\"39.84185\",\"40.23865\",\"41.26198\",\"42.00725\","
				+ "\"42.78946\",\"43.68347\",\"\",\"46.37914\",\"47.68032\",\"\",\"\",\"\",\"\","));
		
		mapReduceDriver.withOutput(new Text("United States : Gross graduation ratio, tertiary, female (%): Missing Years: 2010, 2013, 2014, 2015, ")
    			, new DoubleWritable(0.6156575000000002));
		
		mapReduceDriver.runTest();
	}
}
