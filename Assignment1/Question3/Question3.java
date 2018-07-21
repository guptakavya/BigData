import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Question3 {
	// Mapper class
	public static class NumericOperationsMapper extends Mapper<LongWritable, Text, Text, Text> {
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			context.write(new Text(""), value);
		}

	}

	public static class Combiner extends Reducer<Text, Text, Text, Text> {
		int count = 0;
		int sum = 0;
		int sumSquared = 0;

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			for (Text value : values) {
				int n = Integer.parseInt(value.toString());
				sum += n;
				count++;
				sumSquared += (n * n);
			}
			context.write(key, new Text(sum + "_" + count + "_" + sumSquared));
		}
	}

	// Reducer class
	public static class NumericOperationsReducer extends Reducer<Text, Text, Text, Text> {

		// Reduce function
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			int sumTotal = 0;
			int countTotal = 0;
			int sumSquaredTotal = 0;
			for (Text value : values) {
				String val = value.toString();
				String array[] = val.split("_");
				sumTotal = sumTotal + Integer.parseInt(array[0]);
				countTotal = countTotal + Integer.parseInt(array[1]);
				sumSquaredTotal = sumSquaredTotal + Integer.parseInt(array[2]);
			}

			double meanOfValues = (double) sumTotal / countTotal;
			double X = (double) (Math.pow(sumTotal, 2)) / countTotal;
			double Y = (double) sumSquaredTotal - X;
			double var = (double) Y / countTotal;
			context.write(new Text(""), new Text(meanOfValues + "\t" + var));
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] parameters = new GenericOptionsParser(conf, args).getRemainingArgs();
		/* get all args */
		if (parameters.length != 2) {
			System.err.println("Question3 <NumberInput> <output>");
			System.exit(2);
		}
		// create a job with name mutual friend
		Job job = new Job(conf, "Mean Variance");
		//setting the Jar 
		job.setJarByClass(Question3.class);
		//setting the Mapper 
		job.setMapperClass(NumericOperationsMapper.class);
		//defining the combiner 
		job.setCombinerClass(Combiner.class);
		//setting Reducer 
		job.setReducerClass(NumericOperationsReducer.class);
		//setting the output key 
		job.setOutputKeyClass(Text.class);
		//setting the output value for the class 
		job.setOutputValueClass(Text.class);
		//input data 
		FileInputFormat.addInputPath(job, new Path(parameters[0]));
		//setting the output 
		FileOutputFormat.setOutputPath(job, new Path(parameters[1]));
		//wait for the completion of the job  
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}