package com.mapreduce_code;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class MutualFriendsDriver extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		if (args.length != 2) {
			System.err.println("<input path> <output path>");
			System.exit(-1);
		}

		//Job Setup
		Job fb = Job.getInstance(getConf(), "mutualfriends");
		
		fb.setJarByClass(MutualFriendsDriver.class);
		
		
		//File Input and Output format
		FileInputFormat.addInputPath(fb, new Path(args[0]));
		FileOutputFormat.setOutputPath(fb, new Path(args[1]));
		
		fb.setInputFormatClass(TextInputFormat.class);
		fb.setOutputFormatClass(SequenceFileOutputFormat.class);

		//Mapper-Reducer-Combiner specifications
		fb.setMapperClass(MutualFriendsMapper.class);
		fb.setReducerClass(MutualFriendsReducer.class);
		
		fb.setMapOutputKeyClass(com.custom_writables.FriendPair.class);
		fb.setMapOutputValueClass(com.custom_writables.FriendsArray.class);

		//Output key and value
		fb.setOutputKeyClass(com.custom_writables.FriendPair.class);
		fb.setOutputValueClass(com.custom_writables.FriendsArray.class);
		
		//Submit job
		return fb.waitForCompletion(true) ? 0 : 1;
		
	}

	public static void main(String[] args) throws Exception {
		int ExitCode = ToolRunner.run(new MutualFriendsDriver(), args);
		System.exit(ExitCode);
		
		// TODO Auto-generated method stub

	}

}
