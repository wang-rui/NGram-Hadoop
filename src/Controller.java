import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Controller extends Configured implements Tool {


	/**
	 * Mapper2 reads the output from the first reducer and continue to check if
	 * the first three words exist in the reducer output, if the frequency also
	 * above 5, then set the forth word to the mapper's value and the frequency
	 * to the key (see Reducer 2)
	 **/
	public static class Mapper2 extends
	// KeyIn, ValueIN, KeyOut, ValueOut
			Mapper<LongWritable, Text, LongWritable, Text> {

		/** Map */
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			String line = value.toString();
			 
			Configuration conf4 = context.getConfiguration();
			String input1=conf4.get("word1");
			String input2=conf4.get("word2");
			String input3=conf4.get("word3");


			String word1 = null, word2 = null, word3 = null, word4 = null, freq = null;
			StringTokenizer itr = new StringTokenizer(line);

			if (itr.hasMoreTokens()) {
				word1 = itr.nextToken();
			}
			if (itr.hasMoreTokens()) {
				word2 = itr.nextToken();
			}
			if (itr.hasMoreTokens()) {
				word3 = itr.nextToken();
			}
			if (itr.hasMoreTokens()) {
				word4 = itr.nextToken();
			}
			if (itr.hasMoreTokens()) {
				freq = itr.nextToken();
			}
			if (word3 != null & word4 != null & freq != null) {
				if (word1.equals(input1.toLowerCase())
						&& word2.equals(input2.toLowerCase())
						&& word3.equals(input3.toLowerCase())) {
					context.write(new LongWritable(Long.parseLong(freq)),
							new Text(word4));
				}
				
			
				
			}


		}

	}

	/**
	 * sort the forth word by its possibility
	 **/
	public static class Reducer2 extends
			Reducer<LongWritable, Text, LongWritable, Text> {

		/**
		 * Reduce The objective of this second reducer is to shuffle and sort
		 * the entries based on mapper2's flipping of the data
		 */
		public void reduce(LongWritable key, Text values, Context context)
				throws IOException, InterruptedException {

			context.write(key, values);
		}
	}

	/*
	 * IntComparator reverses the reducer output in a descending order
	 * (http://stackoverflow
	 * .com/questions/8289508/sorting-by-value-in-hadoop-from-a-file)ZZz
	 */
	public static class LongComparator extends WritableComparator {
		public LongComparator() {
			super(LongWritable.class);
		}

		private Long long1;
		private Long long2;

		@Override
		public int compare(byte[] raw1, int offset1, int length1, byte[] raw2,
				int offset2, int length2) {
			long1 = ByteBuffer.wrap(raw1, offset1, length1).getLong();
			long2 = ByteBuffer.wrap(raw2, offset2, length2).getLong();

			return long2.compareTo(long1);
		}

	}
	
	/*
	 * the run method has all the jobs and configuration which have been executed in the main class
	 * (non-Javadoc)
	 * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
	 */

	@Override
	public int run(String[] args) throws Exception {

		String input = args[0];
		String output = "ConvertOutput";
		int numred = 3;//reducer number
	//jobOne create a dictionary file

		Job jobOne = new Job(getConf());
		jobOne.setJobName("Creating dictionary for document collection at "
				+ input + " (Phase 1)");
		jobOne.setJarByClass(this.getClass());

		TextInputFormat.setInputPaths(jobOne, new Path(input));
		TextOutputFormat.setOutputPath(jobOne, new Path(output + "/dic"));

		jobOne.setInputFormatClass(TextInputFormat.class);
		jobOne.setOutputFormatClass(TextOutputFormat.class);

		jobOne.setMapperClass(ConvertText2Input.MapOne.class);
		jobOne.setCombinerClass(ConvertText2Input.CombinerOne.class);
		jobOne.setReducerClass(ConvertText2Input.ReduceOne.class);

		jobOne.setNumReduceTasks(numred);

		jobOne.setMapOutputKeyClass(Text.class);
		jobOne.setMapOutputValueClass(LongWritable.class);
		jobOne.setOutputKeyClass(Text.class);
		jobOne.setOutputValueClass(Text.class);

		jobOne.waitForCompletion(true);

		// /
		// / Job 2: Convert document collection
		// /

		
		Job jobTwo = new Job(getConf());
		jobTwo.setJobName("Converting document collection at " + input
				+ " (Phase 2)");
		jobTwo.setJarByClass(this.getClass());

		TextInputFormat.setInputPaths(jobTwo, new Path(input));
		SequenceFileOutputFormat.setOutputPath(jobTwo,
				new Path(output + "/seq"));
		SequenceFileOutputFormat.setCompressOutput(jobTwo, true);

		jobTwo.setInputFormatClass(TextInputFormat.class);
		jobTwo.setOutputFormatClass(SequenceFileOutputFormat.class);

		jobTwo.setMapperClass(ConvertText2Input.MapTwo.class);

		jobTwo.setNumReduceTasks(0);

		jobTwo.setMapOutputKeyClass(LongWritable.class);
		jobTwo.setMapOutputValueClass(IntArrayWritable.class);
		jobTwo.setOutputKeyClass(LongWritable.class);
		jobTwo.setOutputValueClass(IntArrayWritable.class);

		// add dictionary files to distributed cache
		for (FileStatus file : FileSystem.get(getConf()).listStatus(
				new Path(output + "/dic"))) {
			if (file.getPath().toString().contains("part")) {
				DistributedCache.addCacheFile(file.getPath().toUri(),
						jobTwo.getConfiguration());
			}
		}

		jobTwo.waitForCompletion(true);
		// =====================================================================================================
		long start = System.currentTimeMillis();


		String input2 = "ConvertOutput";
		String output2 = "FinalOutput";
		int minsup = 5;
		int maxlen = 4;
		maxlen = (maxlen == 0 ? Integer.MAX_VALUE : maxlen);

		// parse optional parameters
		int type = 0;
	;

		// delete output directory if it exists
		// FileSystem.get(getConf()).delete(output2), true);

		Job job1 = new Job(getConf());

		// set job name and options
		job1.setJobName("NG-Suffix (" + minsup + ", " + maxlen + ") (" + type
				+ ") (Phase 1)");
		job1.setJarByClass(this.getClass());
		job1.getConfiguration().setInt("de.mpii.ngrams.minsup", minsup);
		job1.getConfiguration().setInt("de.mpii.ngrams.maxlen", maxlen);
		job1.getConfiguration().setInt("de.mpii.ngrams.type", type);

		// set input and output paths
		SequenceFileInputFormat.setInputPaths(job1, new Path(input2 + "/seq"));
		SequenceFileOutputFormat.setOutputPath(job1, new Path(output2
				+ (type == NG.ALL ? "/result" : "/tmp")));

		// set input and output format
		job1.setInputFormatClass(SequenceFileInputFormat.class);
		job1.setOutputFormatClass(SequenceFileOutputFormat.class);

		// set mapper and reducer class
		job1.setMapperClass(NGSuffixSigma.MapOne.class);
		job1.setReducerClass(NGSuffixSigma.ReduceOne.class);

		// set the number of reducers
		job1.setNumReduceTasks(numred);

		// add dictionary to distributed cache
		for (FileStatus file : FileSystem.get(getConf()).listStatus(
				new Path(output + "/dic"))) {
			if (file.getPath().toString().contains("part")) {
				DistributedCache.addCacheFile(file.getPath().toUri(),
						job1.getConfiguration());
			}
		}

		// map output classes
		job1.setMapOutputKeyClass(IntArrayWritable.class);
		job1.setMapOutputValueClass(IntWritable.class);
		job1.setOutputKeyClass(IntArrayWritable.class);
		job1.setOutputValueClass(IntWritable.class);
		job1.setPartitionerClass(IntArrayWritable.IntArrayWritablePartitionerFirstOnly.class);
		job1.setSortComparatorClass(IntArrayWritable.DefaultComparator.class);

		// start job
		job1.waitForCompletion(true);

		
		System.err.println("Took " + (System.currentTimeMillis() - start)
				+ " ms");

		// ===========================================================================================
		// read job parameters from commandline arguments
		String output3 = "FinalOutput/result";
		String textOutput = "FinalTextOutput";

		// delete output directory if it exists
		FileSystem.get(getConf()).delete(new Path(textOutput), true);

		// Job3 convert the output to text
		Job job3 = new Job(getConf());

		// set job name and options
		job3.setJobName("Convert n-grams at " + output3);
		job3.setJarByClass(this.getClass());

		// set input and output paths
		SequenceFileInputFormat.setInputPaths(job3,
				new Path(output3 + "/part*"));
		TextOutputFormat.setOutputPath(job3, new Path(textOutput));

		// set input and output format
		job3.setInputFormatClass(SequenceFileInputFormat.class);
		job3.setOutputFormatClass(TextOutputFormat.class);

		// set mapper and reducer class
		job3.setMapperClass(ConvertOutput2Text.Map.class);

		// set the number of reducers
		job3.setNumReduceTasks(0);

		// add dictionary to distributed cache
		for (FileStatus file : FileSystem.get(getConf()).listStatus(
				new Path(output + "/dic"))) {
			if (file.getPath().toString().contains("part")) {
				DistributedCache.addCacheFile(file.getPath().toUri(),
						job3.getConfiguration());
			}
		}

		// map output classes
		job3.setMapOutputKeyClass(Text.class);
		job3.setMapOutputValueClass(IntWritable.class);

		// start job
		job3.waitForCompletion(true);

		// Create a new JobConf
		Configuration conf4= new Configuration();
		conf4.set("word1", args[1]);
		conf4.set("word2", args[2]);
		conf4.set("word3", args[3]);
		
		Job job4 = new Job(conf4);
		job4.setJarByClass(Controller.class);
		job4.setJobName("NGram");

		job4.setOutputKeyClass(LongWritable.class);
		job4.setOutputValueClass(Text.class);

		job4.setMapperClass(Mapper2.class);
		job4.setCombinerClass(Reducer2.class);
		job4.setReducerClass(Reducer2.class);
		job4.setSortComparatorClass(LongComparator.class);
		job4.setInputFormatClass(TextInputFormat.class);
		job4.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.setInputPaths(job4, new Path("FinalTextOutput"));
		FileOutputFormat.setOutputPath(job4, new Path("FinalMatchingResult"));
		job4.waitForCompletion(true);

		return 0;

	}

	public static void main(String[] args) throws Exception {
		
		int exitCode = ToolRunner.run(new Controller(), args);
		// System.exit(exitCode);

		System.out.println("The top three words have been saved in the FinalMatchingResult Directory");
		
	}
}