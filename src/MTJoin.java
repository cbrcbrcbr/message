package com.chinasofti.hadooptest;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class MTJoin {

	public static int time = 0;

	/**
	 * 在map中先区分输入行属于左表还是右表，然后对两列值进行分割， 保存连接列在key值，剩余列和左右表标志在value中，最后输出
	 */
	public static class Map extends Mapper<Object, Text, Text, Text> {
		// 实现map函数
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();// 每行文件
			String relationtype = "";// 左右表标识
			// 输入文件首行，不处理
			if (line.contains("factoryname") || line.contains("addressed")) {
				return;
			}
			// 输入的一行预处理文本
			String[] datas = line.split(" ");
			String mapkey = datas[0].charAt(0) >= '0'&& datas[0].charAt(0) <= '9' ? datas[0] : datas[1];
			String mapvalue = datas[0].charAt(0) >= '0'	&& datas[0].charAt(0) <= '9' ? datas[1] : datas[0];
			relationtype = datas[0].charAt(0) >= '0'&& datas[0].charAt(0) <= '9' ? "2" : "1";
			// 输出左右表
			context.write(new Text(mapkey), new Text(relationtype + "+"
					+ mapvalue));
		}
	}

	/**
	 * reduce解析map输出，将value中数据按照左右表分别保存，然后求出笛卡尔积，并输出。
	 */
	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		// 实现reduce函数
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			// 输出表头
			if (0 == time) {
				context.write(new Text("factoryname"), new Text("addressname"));
				time++;
			}
			int factorynum = 0, addressnum = 0;
			String[] factory = new String[10], address = new String[10];
			Iterator ite = values.iterator();
			while (ite.hasNext()) {
				String record = ite.next().toString();
				int i = 2;
				if (0 ==  record.length()) {
					continue;
				}
				// 取得左右表标识
				char relationtype = record.charAt(0);
				// 左表
				if ('1' == relationtype) {
					factory[factorynum] = record.substring(i);
					factorynum++;
				}
				// 右表
				if ('2' == relationtype) {
					address[addressnum] = record.substring(i);
					addressnum++;
				}
			}
			// 求笛卡尔积
			if (0 != factorynum && 0 != addressnum) {
				for (int m = 0; m < factorynum; m++) {
					for (int n = 0; n < addressnum; n++) {// 输出结果
						context.write(new Text(factory[m]),	new Text(address[n]));
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
		job.setJarByClass(MTJoin.class);
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
