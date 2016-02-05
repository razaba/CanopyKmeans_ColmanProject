package mypackage;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.StringTokenizer;

import models.ClusterCenter;
import models.DistanceMeasurer;
import models.Vector;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

// first iteration, k-random centers, in every follow-up iteration we have new calculated centers
public class KMeansMapper extends
		Mapper<LongWritable, Text, ClusterCenter, Vector> {

	List<ClusterCenter> centers = new LinkedList<ClusterCenter>();
	List<ClusterCenter> canopies = new LinkedList<ClusterCenter>();
	
	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		super.setup(context);
		
		Configuration conf = context.getConfiguration();
		FileSystem fs = FileSystem.get(conf);
		ClusterCenter key = new ClusterCenter();
		IntWritable value = new IntWritable();
		
		// Read the centroids data
		Path centroids = new Path(conf.get("centroid.path"));
		SequenceFile.Reader reader = new SequenceFile.Reader(fs, centroids, conf);
		while (reader.next(key, value)) {
			centers.add(new ClusterCenter(key.getCanopyId(), key.getCenter()));
		}		
		reader.close();
		
		// Read the canopies data
		Path canopiesPath = new Path(conf.get("canopy.path"));
		reader = new SequenceFile.Reader(fs, canopiesPath, conf);
		while (reader.next(key, value)) {
			canopies.add(new ClusterCenter(key.getCanopyId(), key.getCenter()));
		}		
		reader.close();
	}

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		// Tokenize the line to fit a Vector - every line represents a single Stock
		String line = value.toString();
		StringTokenizer tokenizer = new StringTokenizer(line);
		List<Double> vecValues = new ArrayList<Double>();
		Vector vec = new Vector(tokenizer.nextToken());
		
		while (tokenizer.hasMoreTokens()) {
			vecValues.add(Double.parseDouble(tokenizer.nextToken()));
		}   
	    vec.setVector(ArrayUtils.toPrimitive(vecValues.toArray(new Double[vecValues.size()])));
	    
	    
		// Get the closest canopy
		ClusterCenter nearestCanopy = null;
		double nearestCanopyDist = Double.MAX_VALUE;
		for (ClusterCenter canopy : canopies) {
			double dist = DistanceMeasurer.measureDistance(canopy, vec);
			if (nearestCanopy == null) {
				nearestCanopy = canopy;
				nearestCanopyDist = dist;
			} else {
				if (nearestCanopyDist > dist) {
					nearestCanopy = canopy;
					nearestCanopyDist = dist;
				}
			}
		}
		int canopyId = nearestCanopy.getCanopyId(); 
		
		// Get the closest center related to the specific canopy
		ClusterCenter nearest = null;
		double nearestDistance = Double.MAX_VALUE;
		for (ClusterCenter c : centers) {
			if (c.getCanopyId() == canopyId) {
				double dist = DistanceMeasurer.measureDistance(c, vec);
				if (nearest == null) {
					nearest = c;
					nearestDistance = dist;
				} else {
					if (nearestDistance > dist) {
						nearest = c;
						nearestDistance = dist;
					}
				}
			}
		}
		context.write(nearest, vec);
	}

}
