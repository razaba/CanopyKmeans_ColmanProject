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
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CanopyMapper extends Mapper<LongWritable, Text, IntWritable, ClusterCenter> 
{
	public static enum Counter {
		CanopyId
	}
	
	List<ClusterCenter> centers = new LinkedList<ClusterCenter>();
	IntWritable dummy = new IntWritable(1);
	
	@Override
	protected void map(LongWritable size, Text value, Context context)
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
	    
		int T1 = context.getConfiguration().getInt("canopys.T1", 400);
		
		boolean isInCanopy = false;
		// Check if the vector belongs to an existing center
		for (ClusterCenter c : centers) {
			
			 double dist = DistanceMeasurer.measureDistance(c, vec);

			 if (dist < T1) { 	 
				 context.write(dummy, c);
				 isInCanopy = true; 
				 break;
			 }
		}

		// Add a new canopy center
		if (!isInCanopy) {
			ClusterCenter newCenter = new ClusterCenter(vec);						
			centers.add(newCenter);	
		    context.write(dummy, newCenter);
		}	
	 }
}
