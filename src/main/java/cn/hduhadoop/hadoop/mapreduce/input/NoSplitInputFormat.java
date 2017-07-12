package cn.hduhadoop.hadoop.mapreduce.input;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

public class NoSplitInputFormat extends TextInputFormat {

	@Override
	protected boolean isSplitable(JobContext context, Path file) {
		return false;
	}
	
	
}
