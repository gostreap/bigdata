package inf583.project;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class B_3_hadoop {
	
	public static class Mapper_r0
    extends Mapper<Object, Text, Text, DoubleWritable>{

    private final static DoubleWritable val = new DoubleWritable(1.0/8);
    private Text i = new Text();

		 public void map(Object key, Text value, Context context
		                 ) throws IOException, InterruptedException {
		  
		   String[] line = value.toString().split(" "); 
		   i.set("r "+line[0]);
		   context.write(i, val);
		 }
	}
	
	public static class Mapper_Ar_twoSteps_part1
    extends Mapper<Object, Text, Text,Text>{

    private Text i = new Text();
    private Text j = new Text();

		 public void map(Object key, Text value, Context context
		                 ) throws IOException, InterruptedException {
		   
		   String[] line = value.toString().split(" ");
		   if(line[0].charAt(0) == 'r') {
			   j.set(line[1]);
			   i.set("r "+line[2]);
			   context.write(j, i);
			   
		   }
		   else {
			   i.set(line[0]);
			   for(int k = 1; k <line.length; k++) {
				   j.set(line[k]);
				   context.write(j, i);
			   }
		   }
		  // context.write(j, i);
		 }
		 
    }
	
	 
	public static class Reducer_Ar_twoSteps_part1
    extends Reducer<Text,Text,Text,Text> {
	 	
	 public void reduce(Text key, Iterable<Text> values,
	                    Context context
	                    ) throws IOException, InterruptedException {
	   List<String> i = new ArrayList<String>();
	   Text val = new Text();
	   int size = 0;
	   String test_values = "";
	   for (Text t : values) {
		 String s = t.toString();
		 test_values +=s+";";
	     if(t.charAt(0)=='r') {
	    	 size ++;
	    	 val.set(s.split(" ")[1]);
	    	 
	     }
	     else {
	    	 //val.set(s);
	    	 i.add(s+ " "+key.toString());
	     }
	   }
	   //System.out.println("reducer "+ i + " "+ val );
	   if(size ==1) {
		   Text ij = new Text();
		   for(int k = 0; k< i.size(); k++ ) {
			  ij.set(i.get(k));
			  context.write(ij, val);
		   }
	   }
	   /*else
		   System.out.println("error on reducer: "+test_values);*/
	 }
	}
	
	public static class Mapper_Ar_twoSteps_part2
    extends Mapper<Object, Text, Text,Text>{

    private Text i = new Text();
    private Text val = new Text();

		 public void map(Object key, Text value, Context context
		                 ) throws IOException, InterruptedException {
		   
		   String[] line = value.toString().split(" ");
		   //System.out.println(value.toString());
		   i.set(line[0]);
		   val.set(line[2]);
		   context.write(i, val);
		 }
    }
	
	public static class Reducer_Ar_twoSteps_part2
    extends Reducer<Text,Text,Text,Text> {
	 	
	 public void reduce(Text key, Iterable<Text> values,
	                    Context context
	                    ) throws IOException, InterruptedException {
	
	   Text result = new Text();
	   Double temp_result = 0.0;
	   for (Text val : values) {
		 temp_result += Double.valueOf(val.toString());
	   }
	   //System.out.println("reducer "+ i + " "+ val );
	   result.set(String.valueOf(temp_result));
	   context.write(key, result);
	 }
	}
	
	
	// using only one step
	public static class Mapper_Ar_OneStep
    extends Mapper<Object, Text, Text,Text>{

    private Text i = new Text();
    private Text j = new Text();

		 public void map(Object key, Text value, Context context
		                 ) throws IOException, InterruptedException {
		   
		   String[] line = value.toString().split(" ");
		   if(line[0].charAt(0) == 'r') {
			   i.set("r "+line[2]);
			   for(int k =0; k < A_size; k++) {
				   i.set(String.valueOf(k));
				   j.set("r "+line[1]+" "+line[2]);
				   context.write(i, j);
			   }
			   //context.write(j, i);
		   }
		   else {
			   i.set(line[0]);
			   for(int k = 1; k <line.length; k++) {
				   j.set(line[k]);
				   context.write(i, j);
			   }
		   }
		  // context.write(j, i);
		 }
		 
    }
	
	public static class Reducer_Ar_OneStep
    extends Reducer<Text,Text,Text,Text> {
	 	
	 public void reduce(Text key, Iterable<Text> values,
	                    Context context
	                    ) throws IOException, InterruptedException {
	    
		List<String> from_A = new ArrayList<String>();
		Double[] from_r = new Double[A_size];
	   for (Text val : values) {
		 String s = val.toString();
		 if(s.charAt(0) == 'r') {
			 String[] line = s.split(" ");
			 int j = Integer.parseInt(line[1]);
			 double r_val = Double.parseDouble(line[2]);
			 from_r[j] = r_val;
		 }
		 else {
			 from_A.add(s);
		 }
	   }
	   double  sum = 0.0; 
	   for(int k = 0; k < from_A.size(); k++) {
		   int i = Integer.parseInt(from_A.get(k));
		   sum += from_r[i]; 
	   }
	   context.write(key, new Text(String.valueOf(sum)));
	 }
	}
	
	public static void Ar_twoStep(Configuration conf, String input_path1,String input_path2, String output_path) throws IOException, ClassNotFoundException, InterruptedException {
		 
		 // A*r part 1 (creating (i,j,rj))
		 Job job = Job.getInstance(conf, "A*r_twoStep1");
		 job.getConfiguration().set("mapreduce.output.textoutputformat.separator", " ");
		 job.setJarByClass(B_3_hadoop.class);
		 job.setMapperClass(Mapper_Ar_twoSteps_part1.class);
		 job.setReducerClass(Reducer_Ar_twoSteps_part1.class);
		 job.setOutputKeyClass(Text.class);
		 job.setOutputValueClass(Text.class);
		 FileInputFormat.addInputPaths(job,input_path1+","+input_path2+"/*");
		 FileOutputFormat.setOutputPath(job, new Path(output_path+"_part1"));
		 job.waitForCompletion(true);
		 
		// A*r part 2 (creating (j,rj) = A*r
		 Job job2 = Job.getInstance(conf, "A*r_twoStep2");
		 job2.getConfiguration().set("mapreduce.output.textoutputformat.separator", " ");
		 job2.setJarByClass(B_3_hadoop.class);
		 job2.setMapperClass(Mapper_Ar_twoSteps_part2.class);
		 job2.setReducerClass(Reducer_Ar_twoSteps_part2.class);
		 job2.setOutputKeyClass(Text.class);
		 job2.setOutputValueClass(Text.class);
		 FileInputFormat.addInputPath(job2,new Path(output_path+"_part1/*"));
		 FileOutputFormat.setOutputPath(job2, new Path(output_path));
		 job2.waitForCompletion(true);
	}
	
	public static void Ar_OneStep(Configuration conf, String input_path1,String input_path2, String output_path) throws IOException, ClassNotFoundException, InterruptedException {
		 
		 // A*r part 1 (creating (i,j,rj))
		 Job job = Job.getInstance(conf, "A*r_OneStep1");
		 job.getConfiguration().set("mapreduce.output.textoutputformat.separator", " ");
		 job.setJarByClass(B_3_hadoop.class);
		 job.setMapperClass(Mapper_Ar_OneStep.class);
		 job.setReducerClass(Reducer_Ar_OneStep.class);
		 job.setOutputKeyClass(Text.class);
		 job.setOutputValueClass(Text.class);
		 FileInputFormat.addInputPaths(job,input_path1+","+input_path2+"/*");
		 FileOutputFormat.setOutputPath(job, new Path(output_path));
		 job.waitForCompletion(true);
		 
	}
	
	public static void create_r0(Configuration conf, String input_path, String output_path) throws IOException, ClassNotFoundException, InterruptedException {
		 Job job = Job.getInstance(conf, "r_0");
		 job.getConfiguration().set("mapreduce.output.textoutputformat.separator", " ");
		 job.setJarByClass(B_1_hadoop.class);
		 job.setMapperClass(Mapper_r0.class);
		 //job1.setReducerClass(Reducer_r0.class);
		 job.setOutputKeyClass(Text.class);
		 job.setOutputValueClass(DoubleWritable.class);
		 FileInputFormat.addInputPath(job, new Path(input_path));
		 FileOutputFormat.setOutputPath(job, new Path(output_path));
		 job.waitForCompletion(true);
	}
	
	public static int A_size = 64375;
    
	final static String input1 = "data/graph/edgelist.txt";
	final static String input2 = "data/graph/idslabels.txt";
	final static String out = "B_hadoop_out2";
	
	public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException {
		// TODO Auto-generated method stub
		
		Configuration conf = new Configuration();
		//initialisation
		
		// Creating unormalized r0
		create_r0(conf, input2, out+"/r0");
		
		//A*r in two step
		long timeA = System.currentTimeMillis();
		Ar_twoStep(conf, input1, out+"/r0", out+"/twoStep");
		long timeB = System.currentTimeMillis();
		System.out.println("The duration for two steps multiplication is: "+ (timeB - timeA));
		
		//A*r in One step
		timeA = System.currentTimeMillis();
		Ar_OneStep(conf, input1, out+"/r0", out+"/oneStep");
	    timeB = System.currentTimeMillis();
		
		System.out.println("The duration for one step multiplication is: "+ (timeB- timeA));
		

	}

}
