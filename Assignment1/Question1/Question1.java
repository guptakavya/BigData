import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Question1 {
	// Mapper class
	public static class Map extends Mapper<LongWritable, Text, Text, Text> {

		Text users = new Text();
		Text listOfFriend = new Text();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			// Splitting the values of friends and users
			String[] valueSplit = value.toString().split("\\t");

			// Storing the User Id
			String userId = valueSplit[0];

			if (valueSplit.length == 1) {
				return;
			}

			// Splitting the value of Friends separated by , delimiter
			String[] splitFriends = valueSplit[1].split(",");

			for (String commonFriend : splitFriends) {

				if (userId.equals(commonFriend))
					continue;

				String userKey = (Integer.parseInt(commonFriend) > Integer.parseInt(userId))
						? userId + "\t" + commonFriend : commonFriend + "\t" + userId;

				String regularExp = "((\\b" + commonFriend + ",+\\b)|(\\b+," + commonFriend + "\\b))";
				String ValueOfUser = valueSplit[1].replaceAll(regularExp, "");
				users.set(userKey);
				listOfFriend.set(ValueOfUser);
				context.write(users, listOfFriend);
			}
		}
	}

	// Reducer class
	public static class Reduce extends Reducer<Text, Text, Text, Text> {

		// Key whose output is matched and needs to be printed
		String[] valueOfResult = new String[] { "0\t4", "20\t22939", "1\t29826", "6222\t19272", "28041\t28056" };
		Text result = new Text();
		Text key = new Text();

		public String matchingFriendList(String listOffriends1, String listOfFriends2) {

			// Splitting the first list based on separation by ,
			String[] listTemp1 = listOffriends1.split(",");

			// Splitting the second list based on separation by ,
			String[] listTemp2 = listOfFriends2.split(",");

			// Creating an Arraylist to store common values in two strings
			ArrayList<String> friendshipGroup1 = new ArrayList<String>();
			ArrayList<String> friendshipGroup2 = new ArrayList<String>();
			for (String group : listTemp1) {
				friendshipGroup1.add(group);
			}
			for (String group : listTemp2) {
				friendshipGroup2.add(group);
			}
			friendshipGroup1.retainAll(friendshipGroup2);
			return friendshipGroup1.toString().replaceAll("\\[|\\]", "");
		}

		// Reduce function
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			String[] listOfFriends = new String[2];
			int count = 0;
			for (Text allValues : values) {
				listOfFriends[count] = allValues.toString();
				// counting and incrementing the value by 1
				count = count + 1;
			}
			String friendsMutual = matchingFriendList(listOfFriends[0], listOfFriends[1]);
			result.set(friendsMutual);
			for (String value : valueOfResult) {
				if (key.toString().equals(value)) {
					context.write(key, result);
				}
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] extraParameters = new GenericOptionsParser(conf, args).getRemainingArgs();

		// Gets all the parameters or arguments
		if (extraParameters.length != 2) {
			System.err.println("Question1 <FriendsFile> <output>");
			System.exit(2);
		}

		// Mutual Friend Job
		Job job = new Job(conf, "Question 1");

		// Setting Jar class
		job.setJarByClass(Question1.class);

		// Setting Mapper class
		job.setMapperClass(Map.class);

		// Setting Reducer class
		job.setReducerClass(Reduce.class);

		// Define the output key type
		job.setOutputKeyClass(Text.class);

		// Setting the output value type
		job.setOutputValueClass(Text.class);

		// HDFS for data
		FileInputFormat.addInputPath(job, new Path(extraParameters[0]));

		// HDFS for output
		FileOutputFormat.setOutputPath(job, new Path(extraParameters[1]));

		// Exit the job when it is completed
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}