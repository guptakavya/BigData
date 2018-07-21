import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Question2 {

	// Mapper class
	public static class TopPairsMap extends Mapper<LongWritable, Text, Text, Text> {

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
	public static class TopPairsReducer extends Reducer<Text, Text, Text, Text> {

		Text result = new Text();
		Text key = new Text();

		// Storing the count of mutual friends in HashMap
		LinkedHashMap<String, Integer> map = new LinkedHashMap<String, Integer>();

		// Storing the user and mutual friends in HashMap
		LinkedHashMap<String, String> commonMap = new LinkedHashMap<String, String>();

		// removes values of friends that are unique in each group
		public String matchingFriendList(String listOfFriends1, String listOfFriends2) {
			// splitting the first list to separate string values with comma as
			// delimiter
			String[] listTemp1 = listOfFriends1.split(",");
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
			map.put(key.toString(), friendsMutual.split(",").length);
			commonMap.put(key.toString(), friendsMutual);
		}

		public void cleanup(Context reference) throws IOException, InterruptedException {
			map = valueAfterSort(map);
			int count = 0;
			for (HashMap.Entry<String, Integer> entryToSet : map.entrySet()) {
				if (count == 12)
					break;
				key.set(entryToSet.getKey());
				result.set(entryToSet.getValue().toString());
				reference.write(key, result);
				count = count + 1;
			}
		}

		public LinkedHashMap<String, Integer> valueAfterSort(HashMap<String, Integer> mapToCount) {
			// convert map to a list of map
			List<HashMap.Entry<String, Integer>> list = new LinkedList<HashMap.Entry<String, Integer>>(
					mapToCount.entrySet());
			Collections.sort(list, new Comparator<HashMap.Entry<String, Integer>>() {
				public int compare(HashMap.Entry<String, Integer> entry1, HashMap.Entry<String, Integer> entry2) {
					return -(entry1.getValue().compareTo(entry2.getValue()));
				}
			});

			// Storing the sorted value to the HashMap
			LinkedHashMap<String, Integer> mapAfterSorting = new LinkedHashMap<String, Integer>();
			for (HashMap.Entry<String, Integer> entryToSet : list) {
				mapAfterSorting.put(entryToSet.getKey(), entryToSet.getValue());
			}
			return mapAfterSorting;
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] extraParameters = new GenericOptionsParser(conf, args).getRemainingArgs();

		// Gets all the parameters or arguments
		if (extraParameters.length != 2) {
			System.err.println("Question2 <FriendsFile> <output>");
			System.exit(2);
		}

		// Mutual Friend Job
		Job job = new Job(conf, "Top 10 Pairs");

		// Setting the Jar class
		job.setJarByClass(Question2.class);

		// Setting the Mapper class
		job.setMapperClass(TopPairsMap.class);

		// Setting the Reducer class
		job.setReducerClass(TopPairsReducer.class);

		// Defining the output key class
		job.setOutputKeyClass(Text.class);

		// Setting the output value type
		job.setOutputValueClass(Text.class);

		// HDFS for input data
		FileInputFormat.addInputPath(job, new Path(extraParameters[0]));

		// HDFS for output
		FileOutputFormat.setOutputPath(job, new Path(extraParameters[1]));

		// Exit the job when completed
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}