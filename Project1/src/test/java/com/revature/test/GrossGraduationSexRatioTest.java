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

import com.revature.map.GrossGraduationSexRatioMapper;
import com.revature.reduce.GrossGraduationSexRatioReducer;

public class GrossGraduationSexRatioTest {
	
	private MapDriver<LongWritable, Text, Text, DoubleWritable> mapDriver;
	private ReduceDriver<Text, DoubleWritable, Text, Text> reduceDriver;
	private MapReduceDriver<LongWritable, Text, Text, DoubleWritable, Text, Text> mapReduceDriver;
	
	@Before
	public void setUp() {
		GrossGraduationSexRatioMapper mapper = new GrossGraduationSexRatioMapper();
		mapDriver = new MapDriver<>();
		mapDriver.setMapper(mapper);
		
		GrossGraduationSexRatioReducer reducer = new GrossGraduationSexRatioReducer();
		reduceDriver = new ReduceDriver<>();
		reduceDriver.setReducer(reducer);
		
		mapReduceDriver = new MapReduceDriver<>();
		mapReduceDriver.setMapper(mapper);
		mapReduceDriver.setReducer(reducer);
	}
	
	@Test
	public void testFemaleEmploymentFrom2000Mapper() {
		mapDriver.withInput(new LongWritable(1),new Text("\"United States\",\"USA\",\"Gross graduation ratio, tertiary, "
				+ "female (%)\",\"SE.TER.CMPL.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\""
				+ "\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"35.85857\",\"37.8298\",\"37.43131\",\"38.22037\",\"39.18913\","
				+ "\"39.84185\",\"40.23865\",\"41.26198\",\"42.00725\",\"42.78946\",\"43.68347\",\"\",\"46.37914\",\"47.68032\","
				+ "\"\",\"\",\"\",\"\","));
		
		mapDriver.withOutput(new Text("United States : Gross graduation, tertiary, sex ratio (m/f) : 2000"),
				new DoubleWritable(-37.8298));
		mapDriver.withOutput(new Text("United States : Gross graduation, tertiary, sex ratio (m/f) : 2001"),
				new DoubleWritable(-37.43131));
		mapDriver.withOutput(new Text("United States : Gross graduation, tertiary, sex ratio (m/f) : 2002"),
				new DoubleWritable(-38.22037));
		mapDriver.withOutput(new Text("United States : Gross graduation, tertiary, sex ratio (m/f) : 2003"),
				new DoubleWritable(-39.18913));
		mapDriver.withOutput(new Text("United States : Gross graduation, tertiary, sex ratio (m/f) : 2004"),
				new DoubleWritable(-39.84185));
		mapDriver.withOutput(new Text("United States : Gross graduation, tertiary, sex ratio (m/f) : 2005"),
				new DoubleWritable(-40.23865));
		mapDriver.withOutput(new Text("United States : Gross graduation, tertiary, sex ratio (m/f) : 2006"),
				new DoubleWritable(-41.26198));
		mapDriver.withOutput(new Text("United States : Gross graduation, tertiary, sex ratio (m/f) : 2007"),
				new DoubleWritable(-42.00725));
		mapDriver.withOutput(new Text("United States : Gross graduation, tertiary, sex ratio (m/f) : 2008"),
				new DoubleWritable(-42.78946));
		mapDriver.withOutput(new Text("United States : Gross graduation, tertiary, sex ratio (m/f) : 2009"),
				new DoubleWritable(-43.68347));
		mapDriver.withOutput(new Text("United States : Gross graduation, tertiary, sex ratio (m/f) : 2010"),
				new DoubleWritable(Double.MIN_VALUE));
		mapDriver.withOutput(new Text("United States : Gross graduation, tertiary, sex ratio (m/f) : 2011"),
				new DoubleWritable(-46.37914));
		mapDriver.withOutput(new Text("United States : Gross graduation, tertiary, sex ratio (m/f) : 2012"),
				new DoubleWritable(-47.68032));
		mapDriver.withOutput(new Text("United States : Gross graduation, tertiary, sex ratio (m/f) : 2013"),
				new DoubleWritable(Double.MIN_VALUE));
		mapDriver.withOutput(new Text("United States : Gross graduation, tertiary, sex ratio (m/f) : 2014"),
				new DoubleWritable(Double.MIN_VALUE));
		mapDriver.withOutput(new Text("United States : Gross graduation, tertiary, sex ratio (m/f) : 2015"),
				new DoubleWritable(Double.MIN_VALUE));
		mapDriver.withOutput(new Text("United States : Gross graduation, tertiary, sex ratio (m/f) : 2016"),
				new DoubleWritable(Double.MIN_VALUE));
		
		mapDriver.runTest();
	}
	
	@Test
	public void testChangeInEmploymentReducer() {
		List<DoubleWritable> values = new ArrayList<>();
    	values.add(new DoubleWritable(5));
    	values.add(new DoubleWritable(-5));
    	
    	reduceDriver.withInput(new Text("Gross graduation, tertiary, sex ratio (m/f) : 2000")
    			, values);
    	reduceDriver.withOutput(new Text("Gross graduation, tertiary, sex ratio (m/f) : 2000")
    			, new Text(1.0+""));
    	
    	reduceDriver.runTest();
	}
	
	@Test 
	public void testChangeInEmploymentMapReducer() {
		mapReduceDriver.withInput(new LongWritable(1),new Text("\"United States\",\"USA\",\"Gross graduation ratio, tertiary, "
				+ "female (%)\",\"SE.TER.CMPL.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\""
				+ "\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"35.85857\",\"37.8298\",\"37.43131\",\"38.22037\",\"39.18913\","
				+ "\"39.84185\",\"40.23865\",\"41.26198\",\"42.00725\",\"42.78946\",\"43.68347\",\"\",\"46.37914\",\"47.68032\","
				+ "\"\",\"\",\"\",\"\","));
		mapReduceDriver.withInput(new LongWritable(1),new Text("\"United States\",\"USA\",\"Gross graduation ratio, tertiary, "
				+ "male (%)\",\"SE.TER.CMPL.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\""
				+ "\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"35.85857\",\"37.8298\",\"37.43131\",\"38.22037\",\"39.18913\","
				+ "\"39.84185\",\"40.23865\",\"41.26198\",\"42.00725\",\"42.78946\",\"43.68347\",\"\",\"46.37914\",\"47.68032\","
				+ "\"\",\"\",\"\",\"\","));
		
		mapReduceDriver.withOutput(new Text("United States : Gross graduation, tertiary, sex ratio (m/f) : 2000")
    			, new Text(1.0+""));
		mapReduceDriver.withOutput(new Text("United States : Gross graduation, tertiary, sex ratio (m/f) : 2001")
    			, new Text(1.0+""));
		mapReduceDriver.withOutput(new Text("United States : Gross graduation, tertiary, sex ratio (m/f) : 2002")
    			, new Text(1.0+""));
		mapReduceDriver.withOutput(new Text("United States : Gross graduation, tertiary, sex ratio (m/f) : 2003")
    			, new Text(1.0+""));
		mapReduceDriver.withOutput(new Text("United States : Gross graduation, tertiary, sex ratio (m/f) : 2004")
    			, new Text(1.0+""));
		mapReduceDriver.withOutput(new Text("United States : Gross graduation, tertiary, sex ratio (m/f) : 2005")
    			, new Text(1.0+""));
		mapReduceDriver.withOutput(new Text("United States : Gross graduation, tertiary, sex ratio (m/f) : 2006")
    			, new Text(1.0+""));
		mapReduceDriver.withOutput(new Text("United States : Gross graduation, tertiary, sex ratio (m/f) : 2007")
    			, new Text(1.0+""));
		mapReduceDriver.withOutput(new Text("United States : Gross graduation, tertiary, sex ratio (m/f) : 2008")
    			, new Text(1.0+""));
		mapReduceDriver.withOutput(new Text("United States : Gross graduation, tertiary, sex ratio (m/f) : 2009")
    			, new Text(1.0+""));
		mapReduceDriver.withOutput(new Text("United States : Gross graduation, tertiary, sex ratio (m/f) : 2010"),
				new Text("NaN"));
		mapReduceDriver.withOutput(new Text("United States : Gross graduation, tertiary, sex ratio (m/f) : 2011")
    			, new Text(1.0+""));
		mapReduceDriver.withOutput(new Text("United States : Gross graduation, tertiary, sex ratio (m/f) : 2012")
    			, new Text(1.0+""));
		mapReduceDriver.withOutput(new Text("United States : Gross graduation, tertiary, sex ratio (m/f) : 2013"),
				new Text("NaN"));
		mapReduceDriver.withOutput(new Text("United States : Gross graduation, tertiary, sex ratio (m/f) : 2014"),
				new Text("NaN"));
		mapReduceDriver.withOutput(new Text("United States : Gross graduation, tertiary, sex ratio (m/f) : 2015"),
				new Text("NaN"));
		mapReduceDriver.withOutput(new Text("United States : Gross graduation, tertiary, sex ratio (m/f) : 2016"),
				new Text("NaN"));
		
		mapReduceDriver.runTest();
	}
}
