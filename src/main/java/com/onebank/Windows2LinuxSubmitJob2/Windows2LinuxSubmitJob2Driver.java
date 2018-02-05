package com.onebank.Windows2LinuxSubmitJob2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Windows2LinuxSubmitJob2Driver {

	public static void main(String[] args) throws Exception {
		if(args.length != 3){
			System.err.println("input args counts error!!");
			System.err.println("please input jobName,inputpath,outputpath,date!!");
		    System.exit(0);
		}
		System.out.println(args[0]);
		Configuration conf = new Configuration();
		//conf.set("date", args[3]);
		Job job = Job.getInstance(conf, args[0]);
		
		/*System.out.println(args[1]);
		System.out.println(conf.get("date"));*/
		job.setJarByClass(Windows2LinuxSubmitJob2Driver.class);
		
		job.setMapOutputKeyClass(Text.class);	
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		job.setMapperClass(Windows2LinuxSubmitJob2Map.class);
		job.setReducerClass(Windows2LinuxSubmitJob2Reduce.class);
		
		Path intputPath = new Path(args[1]);
		FileInputFormat.addInputPath(job, intputPath);	
		
		Path outputPath = new Path(args[2]);
		FileOutputFormat.setOutputPath(job, outputPath);	
		
		boolean waitForCompletion = job.waitForCompletion(true);
		System.exit(waitForCompletion ? 0 : 1);
	}
}
