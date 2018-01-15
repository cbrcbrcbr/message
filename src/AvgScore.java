package com.chinasofti.hadooptest;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.chinasofti.hadooptest.MaxTemperature.TempReducer;

public class AvgScore {

	public static class Map extends
			Mapper<LongWritable, Text, Text, IntWritable> {
		// 实现map函数
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			// 将输入的纯文本文件的数据转化成String
			String line = value.toString();
			String[] datas = line.split(" ");
			context.write(new Text(datas[0]),
					new IntWritable(Integer.parseInt(datas[1])));

		}

	}

	public static class Reduce extends
			Reducer<Text, IntWritable, Text, IntWritable> {

		// 实现reduce函数
		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			int count = 0;
			Iterator<IntWritable> iterator = values.iterator();
			while (iterator.hasNext()) {
				sum += iterator.next().get();// 计算总分
				count++;// 统计总的科目数
			}
			int average = (int) sum / count;// 计算平均成绩
			context.write(key, new IntWritable(average));
		}

	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: wordcount <in> <out>");
			System.exit(2);
		}

		Job job = new Job(conf, "word count");
		job.setJarByClass(AvgScore.class);
		job.setMapperClass(Map.class);
		job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
