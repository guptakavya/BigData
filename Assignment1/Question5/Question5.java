import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.*;


public class Question5
{
    static Integer size = new Integer(100);

    // Mapper class
    public static class MatrixMapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
           
        	//Key for the output
        	Text key_output = new Text();
        	//Output value 
            Text output = new Text();
        	String line = value.toString();
            //Splitting the input separated by , delimiter 
            String[] splitInput = line.split(",");
            
            if (splitInput[0].equals("A")){
                for (int i=0; i<size; i++){
                	key_output.set(splitInput[1] + "," + i);
                    output.set(splitInput[0] + "," + splitInput[2] + "," + splitInput[3]);
                    context.write(key_output, output);
                }
            } else{
                for (int j=0; j<size; j++){
                	key_output.set(j + "," + splitInput[2]);
                    output.set(splitInput[0] + "," + splitInput[1] + "," + splitInput[3]);
                    context.write(key_output, output);
                }
            }
        }
    }

    // Reducer class
    public static class MatrixReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context reference) throws IOException, InterruptedException {
            String[] value;

            Integer multiplyValue = new Integer(0);

            int[] matrixM = new int[size];
            int[] matrixN = new int[size];

            for (Text val : values) {
                value = val.toString().split(",");

                if (value[0].equals("A")) {
                    matrixM[Integer.parseInt(value[1])] = Integer.parseInt(value[2]);
                } else {
                    matrixN[Integer.parseInt(value[1])] = Integer.parseInt(value[2]);
                }
            }

            for (int i=0; i<size; i++) {
            	multiplyValue += matrixM[i] * matrixN[i];
            }

            reference.write(key, new Text(multiplyValue.toString()));
        }
    }

    // main function
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] extraparameters = new GenericOptionsParser(conf, args).getRemainingArgs();

        conf.set("mapreduce.output.textoutputformat.separator", ",");

        if (extraparameters.length != 2) {
            System.err.println("Matrix multiplication <input file> <output file>");
            System.exit(2);
        }

        Job job = new Job(conf, "Matrix Multiplication");
        job.setJarByClass(Question5.class);

        job.setMapperClass(MatrixMapper.class);
        job.setReducerClass(MatrixReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(extraparameters[0]));
        FileOutputFormat.setOutputPath(job, new Path(extraparameters[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}