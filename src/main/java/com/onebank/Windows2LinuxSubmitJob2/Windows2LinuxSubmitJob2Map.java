package com.onebank.Windows2LinuxSubmitJob2;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Windows2LinuxSubmitJob2Map extends Mapper<LongWritable, Text, Text, Integer>{

	Text mapKey = null;
	Integer mapValue = 1;
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		mapKey = new Text();
		
	}
	
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		String[] words = line.split(" ");
		for (String word : words) {
			mapKey.set(word.toString());
			context.write(mapKey, mapValue);
		}
	}
}
