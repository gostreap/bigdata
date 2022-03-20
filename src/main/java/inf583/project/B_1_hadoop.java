package inf583.project;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapred.JobConf;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class B_1_hadoop {
	private static Double current_norm = 1.0;

	public static class Mapper_r0 extends Mapper<Object, Text, Text, DoubleWritable> {

		private final static DoubleWritable val = new DoubleWritable(1.0 / 64375);
		private Text i = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			String[] line = value.toString().split(" ");
			i.set("r " + line[0]);
			context.write(i, val);
		}
	}

	public static class Mapper_Ar_part1 extends Mapper<Object, Text, Text, Text> {

		private Text i = new Text();
		private Text j = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			String[] line = value.toString().split(" ");
			if (line[0].charAt(0) == 'r') {
				j.set(line[1]);
				i.set("r " + line[2]);
				context.write(j, i);

			} else {
				i.set(line[0]);
				for (int k = 1; k < line.length; k++) {
					j.set(line[k]);
					context.write(j, i);
				}
			}
			// context.write(j, i);
		}

	}

	public static class Reducer_Ar_part1 extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			List<String> i = new ArrayList<String>();
			Text val = new Text();
			int size = 0;
			for (Text t : values) {
				String s = t.toString();
				if (t.charAt(0) == 'r') {
					size++;
					val.set(s.split(" ")[1]);

				} else {
					// val.set(s);
					i.add(s + " " + key.toString());
				}
			}
			if (size == 1) {
				Text ij = new Text();
				for (int k = 0; k < i.size(); k++) {
					ij.set(i.get(k));
					context.write(ij, val);
				}
			}
		}
	}

	public static class Mapper_Ar extends Mapper<Object, Text, Text, Text> {

		private Text i = new Text();
		private Text val = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			String[] line = value.toString().split(" ");
			i.set(line[0]);
			val.set(line[2]);
			context.write(i, val);
		}
	}

	public static class Reducer_Ar extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			Text result = new Text();
			Double temp_result = 0.0;
			for (Text val : values) {
				temp_result += Double.valueOf(val.toString());
			}
			// System.out.println("reducer "+ i + " "+
			result.set(String.valueOf(temp_result));
			context.write(key, result);
		}
	}

	public static class Mapper_norm extends Mapper<Object, Text, Text, Text> {

		private final Text one = new Text("1");
		private Text val = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			String[] line = value.toString().split(" ");
			// System.out.println(value.toString());
			Double x = Double.parseDouble(line[1]);
			x = x * x;
			val.set(String.valueOf(x));
			context.write(one, val);
		}
	}

	public static class Reducer_norm extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			Double sum = new Double(0.0);
			for (Text val : values) {
				sum += Double.parseDouble(val.toString());
			}
			// System.out.println("reducer "+ i + " "+ val );
			Text result = new Text(String.valueOf(Math.sqrt(sum)));
			current_norm = sum;
			context.write(key, result);
		}
	}

	public static class Mapper_normalize extends Mapper<Object, Text, Text, Text> {

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			String[] line = value.toString().split(" ");
			Double d = Double.parseDouble(line[1]) / current_norm;
			context.write(new Text("r " + line[0]), new Text(String.valueOf(d)));
		}
	}

	public static void create_r0(Configuration conf, String input_path, String output_path)
			throws IOException, ClassNotFoundException, InterruptedException {
		Job job = Job.getInstance(conf, "r_0");
		job.getConfiguration().set("mapreduce.output.textoutputformat.separator", " ");
		job.setJarByClass(B_1_hadoop.class);
		job.setMapperClass(Mapper_r0.class);
		// job1.setReducerClass(Reducer_r0.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		FileInputFormat.addInputPath(job, new Path(input_path));
		FileOutputFormat.setOutputPath(job, new Path(output_path));
		job.waitForCompletion(true);
	}

	public static void Ar(Configuration conf, String input_path1, String input_path2, String output_path)
			throws IOException, ClassNotFoundException, InterruptedException {

		// A*r part 1 (creating (i,j,rj))
		Job job = Job.getInstance(conf, "A*r1");
		job.getConfiguration().set("mapreduce.output.textoutputformat.separator", " ");
		job.setJarByClass(B_1_hadoop.class);
		job.setMapperClass(Mapper_Ar_part1.class);
		job.setReducerClass(Reducer_Ar_part1.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPaths(job, input_path1 + "," + input_path2 + "/*");
		FileOutputFormat.setOutputPath(job, new Path(output_path + "_part1"));
		job.waitForCompletion(true);

		// A*r part 2 (creating (j,rj) = A*r
		Job job2 = Job.getInstance(conf, "A*r2");
		job2.getConfiguration().set("mapreduce.output.textoutputformat.separator", " ");
		job2.setJarByClass(B_1_hadoop.class);
		job2.setMapperClass(Mapper_Ar.class);
		job2.setReducerClass(Reducer_Ar.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job2, new Path(output_path + "_part1/*"));
		FileOutputFormat.setOutputPath(job2, new Path(output_path));
		job2.waitForCompletion(true);
	}

	public static void calculate_norm(Configuration conf, String input_path, String output_path)
			throws IOException, ClassNotFoundException, InterruptedException {
		Job job = Job.getInstance(conf, "norm");
		job.getConfiguration().set("mapreduce.output.textoutputformat.separator", " ");
		job.setJarByClass(B_1_hadoop.class);
		job.setMapperClass(Mapper_norm.class);
		job.setReducerClass(Reducer_norm.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(input_path));
		FileOutputFormat.setOutputPath(job, new Path(output_path));
		job.waitForCompletion(true);
	}

	public static void normalize(Configuration conf, String input_path, String output_path)
			throws IOException, ClassNotFoundException, InterruptedException {

		// A*r part 1 (creating (i,j,rj))
		Job job = Job.getInstance(conf, "normalisation");
		job.getConfiguration().set("mapreduce.output.textoutputformat.separator", " ");
		job.setJarByClass(B_1_hadoop.class);
		job.setMapperClass(Mapper_normalize.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(input_path));
		FileOutputFormat.setOutputPath(job, new Path(output_path));
		job.waitForCompletion(true);
	}

	final static String input1 = "data/graph/edgelist2.txt";
	final static String input2 = "data/graph/idslabels.txt";
	final static String out = "B_hadoop_out";

	public static void question1_hadoop(int T) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();

		// initialisation

		// Creating unormalized r0
		create_r0(conf, input2, out + "/iteration_0/r_normalized");

		for (int t = 1; t < T; t++) {
			// Calculating product A*r
			Ar(conf, input1, out + "/iteration_" + (t - 1) + "/r_normalized", out + "/iteration_" + (t) + "/Ar");
			// Calculing norm
			calculate_norm(conf, out + "/iteration_" + (t) + "/Ar", out + "/iteration_" + (t) + "/norm");
			// normalizing
			normalize(conf, out + "/iteration_" + (t) + "/Ar", out + "/iteration_" + (t) + "/r_normalized");
		}
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		long timeA = System.currentTimeMillis();
		question1_hadoop(10);
		long timeB = System.currentTimeMillis();
		System.out.println("Time question1 hadoop: " + (timeB - timeA));
	}

}
