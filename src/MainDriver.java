package mypackage;
import java.io.IOException;
import java.util.*;
        
import models.Vector;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
public class MainDriver {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception  {
		  Configuration conf = new Configuration();
	        
		    Job job = new Job(conf, "CanopyMapReduce");
		    
		    
		    job.setOutputKeyClass(Vector.class);
		    job.setOutputValueClass(IntWritable.class);
		        
		    job.setMapperClass(CanopyMapper.class);
		    job.setReducerClass(CanopyReducer.class);
		        
		    job.setInputFormatClass(TextInputFormat.class);
		    job.setOutputFormatClass(TextOutputFormat.class);
		        
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		    
		    job.setJarByClass(CanopyMapper.class); 
		    job.waitForCompletion(true);

	}

}
