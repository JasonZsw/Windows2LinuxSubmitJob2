package com.onebank.Windows2LinuxSubmitJob2;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Windows2LinuxSubmitJob2Map extends Mapper<LongWritable, Text, Text, Integer>{

	Text mapKey = null;
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		Text mapKey = new Text();
		
	}
	
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		
	}
}
