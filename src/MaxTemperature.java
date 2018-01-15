package com.chinasofti.hadooptest;

import java.io.IOException;
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

public class MaxTemperature {
	static class TempMapper extends

	Mapper<LongWritable, Text, Text, IntWritable> {

		@Override
		public void map(LongWritable key, Text value, Context context)

		throws IOException, InterruptedException {
			// 打印样本: Before Mapper: 0, 2000010115
			System.out.print("Before Mapper: " + key + ", " + value);
			String line = value.toString();
			String year = line.substring(0, 4);
			int temperature = Integer.parseInt(line.substring(8));
			context.write(new Text(year), new IntWritable(temperature));
			// 打印样本: After Mapper:2000, 15
			System.out.println("======" + "After Mapper:" + new Text(year)
					+ ", " + new IntWritable(temperature));
		}
	}

	/**
	 * 
	 * 四个泛型类型分别代表：
	 * 
	 * KeyIn Reducer的输入数据的Key，这里是每行文字中的“年份”
	 * 
	 * ValueIn Reducer的输入数据的Value，这里是每行文字中的“气温”
	 * 
	 * KeyOut Reducer的输出数据的Key，这里是不重复的“年份”
	 * 
	 * ValueOut Reducer的输出数据的Value，这里是这一年中的“最高气温”
	 */

	static class TempReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		@Override
		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int maxValue = Integer.MIN_VALUE;
			StringBuffer sb = new StringBuffer();
			// 取values的最大值
			for (IntWritable value : values) {
				maxValue = Math.max(maxValue, value.get());
				sb.append(value).append(", ");
			}
			// 打印样本： Before Reduce: 2000, 15, 23, 99, 12, 22,
			System.out.print("Before Reduce: " + key + ", " + sb.toString());
			context.write(key, new IntWritable(maxValue));
			// 打印样本： After Reduce: 2000, 99
			System.out.println("======" + "After Reduce: " + key + ", "
					+ maxValue);
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
		job.setJarByClass(MaxTemperature.class);
		job.setMapperClass(TempMapper.class);
		job.setCombinerClass(TempReducer.class);
		job.setReducerClass(TempReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
