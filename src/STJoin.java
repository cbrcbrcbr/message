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

public class STJoin {

	public static int time = 0;

	/* *
	 * map将输出分割child和parent，然后正序输出一次作为右表，
	 * 反序输出一次作为左表，需要注意的是在输出的value中必须加上左右表的区别标识。
	 */

	public static class Map extends Mapper<Object, Text, Text, Text> {
		// 实现map函数
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String childname = "";// 孩子名称
			String parentname = "";// 父母名称
			String relationtype = "";// 左右表标识
			// 输入的一行预处理文本
			String[] values = value.toString().split(" ");
			if (!values[0].equals("child")) {// 忽略表头
				childname = values[0];
				parentname = values[1];
				// 输出左表
				relationtype = "1";
				context.write(new Text(values[1]), new Text(relationtype + "+"
						+ childname + "+" + parentname));
				// 输出右表
				relationtype = "2";
				context.write(new Text(values[0]), new Text(relationtype + "+"
						+ childname + "+" + parentname));
			}
		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		// 实现reduce函数
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			if (0 == time) {// 输出表头
				context.write(new Text("grandchild"), new Text("grandparent"));
				time++;
			}
			int grandchildnum = 0, grandparentnum = 0;
			String[] grandchild = new String[10], grandparent = new String[10];
			Iterator ite = values.iterator();
			while (ite.hasNext()) {
				String record = ite.next().toString();
				if (0 == record.length()) {
					continue;
				}
				String[] datas = record.split("\\+");
				char relationtype = datas[0].charAt(0); // 取得左右表标识
				String childname = "", parentname = ""; // 定义孩子和父母变量
				childname = datas[1];// 获取value-list中value的child
				parentname = datas[2];// 获取value-list中value的parent			
				if ('1' == relationtype) {	// 左表，取出child放入grandchildren
					grandchild[grandchildnum++] = childname;					
				}				
				if ('2' == relationtype) {// 右表，取出parent放入grandparent
					grandparent[grandparentnum++] = parentname;					
				}
			}
			// grandchild和grandparent数组求笛卡尔儿积
			if (0 != grandchildnum && 0 != grandparentnum) {
				for (int m = 0; m < grandchildnum; m++) {
					for (int n = 0; n < grandparentnum; n++) {
						context.write(new Text(grandchild[m]), new Text(
								grandparent[n]));
					}
				}
			}
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
		job.setJarByClass(STJoin.class);
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
