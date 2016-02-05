package mypackage;
import java.io.IOException;


import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import models.ClusterCenter;
import models.DistanceMeasurer;
import models.Vector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.retry.RetryPolicies.MultipleLinearRandomRetry.Pair;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;
import java.util.Map;
import java.util.Map.Entry;


public class CanopyReducer  extends Reducer<IntWritable, ClusterCenter, ClusterCenter, IntWritable> 
{
	private static final Log LOG = LogFactory.getLog(KMeansClusteringJob.class);
	
	List<ClusterCenter> canopies = new LinkedList<ClusterCenter>();
	
	@Override
	protected void reduce(IntWritable key, Iterable<ClusterCenter> values,
			Context context) throws IOException, InterruptedException {
		
		// Count the number of points related to each possible center
		HashMap<ClusterCenter, Integer> centersCounters = new HashMap<ClusterCenter, Integer>();
		
        for (ClusterCenter center : values) {
			if (centersCounters.containsKey(center)) {
				centersCounters.put(new ClusterCenter(center.getCanopyId(),center.getCenter()), centersCounters.get(center) + 1);
			} else {
				centersCounters.put(new ClusterCenter(center.getCanopyId(),center.getCenter()), 1);
			}
        }
       
        // Run the canopys algorithm again, this time on the possible centers from the Map phase
		int T1 = context.getConfiguration().getInt("canopys.T1", 1000);
		
		HashMap<ClusterCenter, Integer> finalCanopys = new HashMap<ClusterCenter, Integer>();
		
		int canopyId = 1;
		int totalVectors = 0;
		for (ClusterCenter vec : centersCounters.keySet()) {
			
			ClusterCenter foundCenter = null;
			
			// Check if the vector belongs to an existing center
			for (ClusterCenter center : canopies) {	
				 double dist = DistanceMeasurer.measureDistance(center, vec);
				 
				 // Check if is in radius
				 if (dist < T1) {
					 foundCenter = center;			 
					 break;
				 }
			}
			
			totalVectors += centersCounters.get(vec);
			// The vector belongs to an existing center, sum up the points
			if (foundCenter != null){
				finalCanopys.put(new ClusterCenter(foundCenter.getCanopyId(),foundCenter.getCenter()), finalCanopys.get(foundCenter) + centersCounters.get(vec));
			} else {
				// Add a new canopy center
				ClusterCenter vector = new ClusterCenter(vec.getCanopyId(),vec.getCenter());
				vector.setCanopyId(canopyId++);
				canopies.add(vector);
				finalCanopys.put(vector, centersCounters.get(vector));
			}
		}
		
		int K = context.getConfiguration().getInt("KMeans.K", 5);
		
		final IntWritable zero = new IntWritable(0);
		
		// Calculate the number of centers for each canopy and write the center
		int numOfCentersPerCanopy = 0;
		int totalCenters = 0;
		for (Entry<ClusterCenter, Integer> entry : finalCanopys.entrySet()){	
			numOfCentersPerCanopy = Math.round((float)entry.getValue() / (float)totalVectors * K);
			if (totalCenters + numOfCentersPerCanopy > K) {
				numOfCentersPerCanopy = K - totalCenters;
			}
			totalCenters += numOfCentersPerCanopy;
			
			if (numOfCentersPerCanopy == 0) {
				if (canopies.remove(entry.getKey()) == false){
					LOG.error("Canopy wasnt removed properly");//throw new Exception("Canopy wasnt removed properly");
				}
			}
			
			for (int canopyNum = 0; canopyNum < numOfCentersPerCanopy; canopyNum++) {
				Vector randVector = getRandomVector(entry.getKey().getCenter().getVector().length); 
				context.write(new ClusterCenter(entry.getKey().getCanopyId(), randVector), zero);
			}
		}
	}
	
	Random rand = new Random();
	private Vector getRandomVector(int size) {
		double[] values = new double[size];
		for (int index = 0; index < size; index++) {
			values[index] = rand.nextInt(101);
		}
		Vector vec = new Vector();
		vec.setVector(values);
		return vec;
	}
	
	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		super.cleanup(context);
		Configuration conf = context.getConfiguration();
		Path outPath = new Path(conf.get("canopy.path"));//"files/clustering/center/canopies.seq");
		FileSystem fs = FileSystem.get(conf);
		fs.delete(outPath, true);
		final SequenceFile.Writer out = SequenceFile.createWriter(fs,
				context.getConfiguration(), outPath, ClusterCenter.class,
				IntWritable.class);
		final IntWritable value = new IntWritable(0);
		for (ClusterCenter center : canopies) {
			out.append(center, value);
		}
		out.close();
	}
}
