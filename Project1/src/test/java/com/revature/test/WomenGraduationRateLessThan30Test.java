package com.revature.test;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

import com.revature.map.WomenGraduationLessThanThirtyMapper;

public class WomenGraduationRateLessThan30Test {
	
	private MapDriver<LongWritable, Text, Text, Text> mapDriver;
	
	@Before
	public void setUp() {
		WomenGraduationLessThanThirtyMapper mapper = new WomenGraduationLessThanThirtyMapper();
		mapDriver = new MapDriver<>();
		mapDriver.setMapper(mapper);
	}
	
	@Test
	public void testFemaleEmploymentFrom2000Mapper() {
		mapDriver.withInput(new LongWritable(1),new Text("\"Mauritania\",\"MRT\",\"Gross graduation ratio, "
				+ "tertiary, female (%)\",\"SE.TER.CMPL.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\""
				+ ",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\""
				+ ",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\""
				+ ",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"2.8299\",\"2.41616\","));
		
		mapDriver.withOutput(new Text("Mauritania : Gross graduation ratio, tertiary, female (%) : 2015"),
				new Text(2.8299+""));
		
		mapDriver.runTest();
	}
}
