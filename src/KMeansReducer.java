package mypackage;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import models.ClusterCenter;
import models.Vector;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

// calculate a new cluster center for these vertices
public class KMeansReducer extends
		Reducer<ClusterCenter, Vector, ClusterCenter, IntWritable> {

	public static enum Counter {
		CONVERGED,
		K_CENTER
	}

	List<ClusterCenter> centers = new LinkedList<ClusterCenter>();
	IntWritable dummy = new IntWritable(0);
	
	@Override
	protected void reduce(ClusterCenter key, Iterable<Vector> values,
			Context context) throws IOException, InterruptedException {
		if (!context.getConfiguration().getBoolean("centroid.last", false)) {
			Vector newCenter = new Vector();
			List<Vector> vectorList = new LinkedList<Vector>();
			int vectorSize = key.getCenter().getVector().length;
			newCenter.setVector(new double[vectorSize]);
			for (Vector value : values) {
				vectorList.add(new Vector(value));
				for (int i = 0; i < value.getVector().length; i++) {
					newCenter.getVector()[i] += value.getVector()[i];
				}
			}
	
			for (int i = 0; i < newCenter.getVector().length; i++) {
				newCenter.getVector()[i] = newCenter.getVector()[i]
						/ vectorList.size();
			}
	
			ClusterCenter center = new ClusterCenter(key.getCanopyId(),newCenter);
			centers.add(center);
			context.write(center, dummy);
	
			if (center.converged(key))
				context.getCounter(Counter.CONVERGED).increment(1);
		} else {
			context.getCounter(Counter.K_CENTER).increment(1);
			int k = (int) context.getCounter(Counter.K_CENTER).getValue();
			for (Vector vector : values) {
				context.write(new ClusterCenter(vector),new IntWritable(k));
			}
		}
	}
}
