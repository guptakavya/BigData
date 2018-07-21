import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class Question4 {
	// Mapper class 
	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> 
	{
		private final static IntWritable tempWritable = new IntWritable(0);
		public void map(LongWritable key, Text value, Context output)
				throws IOException, InterruptedException {
			String val = value.toString();
			int tempValue = Integer.parseInt(val);
			tempWritable.set(tempValue);
			output.write(new Text(""), tempWritable);
		}
	}

	// Reducer class 
	public static class Reduce extends Reducer<Text, IntWritable, Text, Text> {

		ArrayList<Integer> number = new ArrayList<>();
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			double medianValue =0.0;
			int maximum=Integer.MIN_VALUE;
			int minimum=Integer.MAX_VALUE;
			for(IntWritable value:values){
				number.add(value.get());
			}
			Collections.sort(number);
			minimum = number.get(0);

			maximum = number.get(number.size()-1);
			if(number.size()%2==0){

				int value1 = number.get(number.size()/2);
				int value2 = number.get((number.size()/2)-1);
				medianValue = (double)(value1+value2)/2;
		
			}else{

				medianValue = number.get(number.size()/2);	
			}
			context.write(key, new Text(minimum+"\t"+maximum+"\t"+medianValue));

		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] parameters = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		
		if (parameters.length != 2) {
			System.err.println("Question4 <InputFile> <output>");
			System.exit(2);
		}
		Job task = new Job(conf, "Minimum Maximum Median");
		task.setJarByClass(Question4.class);
		task.setMapperClass(Map.class);
		task.setReducerClass(Reduce.class);
		task.setOutputKeyClass(Text.class);
		task.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(task, new Path(parameters[0]));
		FileOutputFormat.setOutputPath(task, new Path(parameters[1]));
		System.exit(task.waitForCompletion(true) ? 0 : 1);
	}
}