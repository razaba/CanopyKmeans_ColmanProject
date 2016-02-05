package mypackage;
import java.io.IOException;

import javax.security.auth.callback.TextOutputCallback;

import models.ClusterCenter;
import models.Vector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class KMeansClusteringJob {

	private static final Log LOG = LogFactory.getLog(KMeansClusteringJob.class);

	public static void main(String[] args) throws IOException,
			InterruptedException, ClassNotFoundException {
		
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		
		// -------------------------- Canopys Job Start -------------------------

		// Set the parameters for the canopys job
	    // TODO: integration - handle the real Stocks files
	    Path initialInput = new Path(args[0]); //new Path("files/clustering/import/nasdaq2.txt");
	    conf.set("input.path", initialInput.toString());
		int vecLength = Integer.parseInt(args[3]);
		conf.setInt("canopys.T1", (int)(Math.ceil(((double)vecLength)/10) * 400));
		int K = Integer.parseInt(args[2]);
		conf.setInt("KMeans.K", K);
		Path centroids = new Path("files/clustering/center/centroids");
		conf.set("centroid.path", centroids.toString());
		Path canopies = new Path("files/clustering/center/canopies.seq");
		conf.set("canopy.path", canopies.toString());
		LOG.info(conf.get("canopy.path"));
		
	    Job canopiesJob = new Job(conf);
	    canopiesJob.setJobName("CanopyMapReduce");
	    
	    canopiesJob.setMapOutputKeyClass(IntWritable.class);
	    canopiesJob.setMapOutputValueClass(ClusterCenter.class);
	    
	    canopiesJob.setOutputKeyClass(ClusterCenter.class);
	    canopiesJob.setOutputValueClass(IntWritable.class);
	    
	    canopiesJob.setMapperClass(CanopyMapper.class);
	    canopiesJob.setReducerClass(CanopyReducer.class);
	        
	    canopiesJob.setInputFormatClass(TextInputFormat.class);
	    canopiesJob.setOutputFormatClass(SequenceFileOutputFormat.class);
	    
	    FileInputFormat.addInputPath(canopiesJob, initialInput);
	    SequenceFileOutputFormat.setOutputPath(canopiesJob, centroids);
	    
	    canopiesJob.setJarByClass(CanopyMapper.class); 
	    
		if (fs.exists(canopies))
			fs.delete(canopies, true);
		
		if (fs.exists(centroids))
			fs.delete(centroids, true);
		
		// Run the canopy clustering algorithm
	    canopiesJob.waitForCompletion(true);    
	    // -------------------------- Canopies Job End -------------------------
	    
	    // -------------------------- K-Means Job Start -------------------------  
		int iteration = 1;

		centroids = new Path("files/clustering/center/centroids/part-r-00000");
		Path out = new Path("files/clustering/depth_" + iteration);
		Path result = new Path("files/clustering/result.txt");
		
		Job kmeansJob = null;
	    long counter = 0;
		do  {
			conf = new Configuration();	
			conf.set("centroid.path", centroids.toString());
			conf.set("canopy.path", canopies.toString());
			conf.set("num.iteration", iteration + "");
			
			if (fs.exists(out))
				fs.delete(out, true);
			
			if (fs.exists(result))
				fs.delete(result, true);
			
			kmeansJob = new Job(conf);
			kmeansJob.setJobName("KMeans Clustering " + iteration);
			
			kmeansJob.setMapperClass(KMeansMapper.class);
			kmeansJob.setReducerClass(KMeansReducer.class);
			
			kmeansJob.setMapOutputKeyClass(ClusterCenter.class);
			kmeansJob.setMapOutputValueClass(Vector.class);
			
			kmeansJob.setOutputKeyClass(ClusterCenter.class);
			kmeansJob.setOutputValueClass(IntWritable.class);
			
			kmeansJob.setInputFormatClass(TextInputFormat.class);
			FileInputFormat.addInputPath(kmeansJob, initialInput);
			
			kmeansJob.setOutputFormatClass(SequenceFileOutputFormat.class);
			SequenceFileOutputFormat.setOutputPath(kmeansJob, out);
			
			kmeansJob.setJarByClass(KMeansMapper.class);
			
			kmeansJob.waitForCompletion(true);
			
			iteration++;
			centroids = new Path("files/clustering/depth_" + (iteration - 1) + "/part-r-00000");
			out = new Path("files/clustering/depth_" + iteration);
			
			counter = kmeansJob.getCounters().findCounter(KMeansReducer.Counter.CONVERGED).getValue();
		} while (counter > 0);
		
		conf = new Configuration();
		conf.set("num.iteration", iteration + "");
		conf.set("centroid.path", centroids.toString());
		conf.set("canopy.path", canopies.toString());
		conf.setBoolean("centroid.last", true);
		
		out = new Path(args[1]);//"files/clustering/output");
		
		if (fs.exists(out))
			fs.delete(out, true);
		
		kmeansJob = new Job(conf, "Last KMeans Clustering");

		kmeansJob.setOutputKeyClass(ClusterCenter.class);
		kmeansJob.setOutputValueClass(Vector.class);
		
		kmeansJob.setMapperClass(KMeansMapper.class);
		kmeansJob.setReducerClass(KMeansReducer.class);


		kmeansJob.setInputFormatClass(TextInputFormat.class);
		kmeansJob.setOutputFormatClass(TextOutputFormat.class);
	        
	    FileInputFormat.addInputPath(kmeansJob, initialInput);
	    FileOutputFormat.setOutputPath(kmeansJob, out);	 
	    
		kmeansJob.setJarByClass(KMeansMapper.class);
		
		kmeansJob.waitForCompletion(true);
		
		// -------------------------- K-Means Job End -------------------------
	}

}
