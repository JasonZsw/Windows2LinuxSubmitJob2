package com.onebank.Windows2LinuxSubmitJob2;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Windows2LinuxSubmitJob2Reduce extends Reducer<Text, Integer, Text, NullWritable>{

	Text mapKey = null;
	int counts = 0 ;
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		mapKey = new Text();
	}
	
	@Override
	protected void reduce(Text key, Iterable<Integer> value, Context context)
			throws IOException, InterruptedException {
		counts = 0 ;
		for (Integer count : value) {
			counts = counts + count;
		}
		mapKey.set(key.toString()+" "+counts);
		context.write(mapKey, null);
	
	}
	
}
