/*  Edward Cox
	EJC71
	Referenced: https://hadoop.apache.org/docs/stable/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html
				https://hadoop.apache.org/docs/r2.7.0/api/org/apache/hadoop/mapred/FileSplit.html
				https://www.geeksforgeeks.org/java-util-hashmap-in-java-with-examples/
				https://www.geeksforgeeks.org/iterate-map-java/
				https://www.w3schools.com/java/java_arraylist.asp
				https://www.geeksforgeeks.org/collections-sort-java-examples/
				https://www.geeksforgeeks.org/convert-an-arraylist-of-string-to-a-string-array-in-java/
				https://www.geeksforgeeks.org/arrays-tostring-in-java-with-examples/
*/

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.Collections;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class InvertedIndex
{
	public static class InvertedIndexMapper extends Mapper<LongWritable, Text, Text, Text>
	{
		private Text word = new Text();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			// Read and tokenize input line by line
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);

			// Get document id
			String name = ((FileSplit)context.getInputSplit()).getPath().getName();

			// Iterate through
			while (tokenizer.hasMoreTokens())
			{
				word.set(tokenizer.nextToken());

				/*
					'word1' Miserables.txt
					'word1' Miserables.txt
					'word2' NotreDame_De_Paris.txt
				*/
				context.write(word, new Text(name));
			}
		}
	}

	public static class InvertedIndexReducer extends Reducer<Text, Text, Text, Text>
	{
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		{
			//int sum = 0;
			HashMap<String, Integer> hm = new HashMap<>();

			// Iterate through to populate HashMap
			for (Text value : values)
			{
				// If value is not in hashmap yet, put value as key and number of occurences as value
				if (!hm.containsKey(value.toString()))
				{
					hm.put(value.toString(), 1);
				}
				// If value is already in hashmap, increase its counter of occurences
				else
				{
					hm.put(value.toString(), (int)hm.get(value.toString()) + 1);
				}
				// sum += value.get();
			}

			// First step from notes
			// Iterate through HashMap and append all listings associated with same key
			ArrayList<String> a = new ArrayList<String>();
			for (String s : hm.keySet())
			{
				a.add(s + " " + hm.get(s) + ", ");

			}

			// Second step from notes
			// Sort
			Collections.sort(a);

			String combine = "";
			for (int i=0; i<a.size(); i++)
			{
				combine += a.get(i);
			}

			/* Third step from notes
				'word1' Miserables.txt 1, NotreDame_De_Paris.txt 2,
				'word1' Miserables.txt 2, NotreDame_De_Paris.txt 1,
				'word2' NotreDame_De_Paris.txt 1, Miserables.txt 1,
			*/
			context.write(key, new Text(combine));
		}
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException
	{
		if (args.length != 2)
		{
			System.err.println("Usage: Inverted Index <input> <output>");
			System.exit(-1);
		}

		//Delete results folder from last run
		try {
			ProcessBuilder pb1 = new ProcessBuilder("/usr/lib/hadoop/bin/hadoop", "fs", "-rm", "-r", "gs://dataproc-staging-us-west1-628394627960-6e5uyn8v/new");
			Process p1 = pb1.start();
			p1.waitFor();
		} catch (Exception e) {
		}

		//Creating hadoop job and assigning a job name for identification
		Job job = new Job();
		job.setJarByClass(InvertedIndex.class);
		job.setJobName("Inverted Index");

		//HDFS input and output directories to be fetched from the Dataproc job submission console
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		//Providing mapper and reducer class names
		job.setMapperClass(InvertedIndexMapper.class);
		job.setReducerClass(InvertedIndexReducer.class);

		//Setting the job object with the data types of output key(Text) and value(IntWritable)
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.waitForCompletion(true);

		// Clean up old files
		try {
			ProcessBuilder pb2 = new ProcessBuilder("/usr/lib/hadoop/bin/hadoop", "fs", "-rm", "gs://dataproc-staging-us-west1-628394627960-6e5uyn8v/output.txt");
			Process p2 = pb2.start();
			p2.waitFor();
		} catch (Exception e) {
		}

		try {
			ProcessBuilder pb3 = new ProcessBuilder("rm", "output.txt");
			Process p3 = pb3.start();
			p3.waitFor();
		} catch (Exception e) {
		}

		try {
			ProcessBuilder pb4 = new ProcessBuilder("/usr/lib/hadoop/bin/hadoop", "fs", "-rm", "output.txt");
			Process p4 = pb4.start();
			p4.waitFor();
		} catch (Exception e) {
		}

		// Delete '_SUCCESS'
		try {
			ProcessBuilder pb5 = new ProcessBuilder("/usr/lib/hadoop/bin/hadoop", "fs", "-rm", "gs://dataproc-staging-us-west1-628394627960-6e5uyn8v/new/_SUCCESS");
			Process p5 = pb5.start();
			p5.waitFor();
		} catch (Exception e) {
		}

		// Merge files
		try {
			ProcessBuilder pb6 = new ProcessBuilder("/usr/lib/hadoop/bin/hadoop", "fs", "-getmerge", "gs://dataproc-staging-us-west1-628394627960-6e5uyn8v/new", "/home/coxej1/output.txt");
			Process p6 = pb6.start();
			p6.waitFor();
		} catch (Exception e) {		
		}

		// Copy from local
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Path localPath = new Path("/home/coxej1/output.txt");
		Path hdfsPath = new Path("./output.txt");
		fs.copyFromLocalFile(localPath, hdfsPath);

		// Copy to bucket
		try {
			ProcessBuilder pb7 = new ProcessBuilder("/usr/lib/hadoop/bin/hadoop", "fs", "-cp", "./output.txt", "gs://dataproc-staging-us-west1-628394627960-6e5uyn8v/output.txt");
			Process p7 = pb7.start();
			p7.waitFor();
		} catch (Exception e) {
		}
	}
}