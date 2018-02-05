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
	/*	if(args.length != 3){
			System.err.println("input args counts error!!");
			System.err.println("please input jobName,inputpath,outputpath,date!!");
		    System.exit(0);
		}*/
		//System.out.println(args[0]);
		Configuration conf = new Configuration();
		
//		conf.setBoolean("mapreduce.app-submission.cross-platform", true);// 配置使用跨平台提交任务
//		
//		conf.set("fs.defaultFS", "hdfs://192.168.230.102:9000/test000"); // 指定namenode
//		
//		conf.set("mapreduce.framework.name", "yarn"); // 指定使用yarn框架
//		
//		conf.set("yarn.resourcemanager.address", "192.168.230.101:8032"); // 指定ResourceManager
//		
//		conf.set("yarn.resourcemanager.scheduler.address", "192.168.230.101:8030");// 指定资源分配器  
		
		
		
		//conf.set("date", args[3]);
		Job job = Job.getInstance(conf, "test2");
		
		/*System.out.println(args[1]);
		System.out.println(conf.get("date"));*/
		job.setJarByClass(Windows2LinuxSubmitJob2Driver.class);
		
		job.setJar("F:\\eeeee\\Windows2LinuxSubmitJob2.jar");
		
		job.setMapOutputKeyClass(Text.class);	
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		job.setMapperClass(Windows2LinuxSubmitJob2Map.class);
		job.setReducerClass(Windows2LinuxSubmitJob2Reduce.class);
		
		Path intputPath = new Path("hdfs://192.168.230.102:9000/test000/input/word.txt");
		FileInputFormat.addInputPath(job, intputPath);	
		
		Path outputPath = new Path("hdfs://192.168.230.102:9000/test000/output");
		FileOutputFormat.setOutputPath(job, outputPath);	
		
		boolean waitForCompletion = job.waitForCompletion(true);
		System.exit(waitForCompletion ? 0 : 1);
	}
}
