package models;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

public class ClusterCenter implements WritableComparable<ClusterCenter> {

	private Vector center;
	private IntWritable canopyId;
	
	public ClusterCenter() {
		super();
		this.center = null;
		this.canopyId = new IntWritable();
	}
	
	public ClusterCenter(Vector center) {
		super();
		this.center = center;
		this.canopyId = new IntWritable();
	}

	public ClusterCenter(int canopyId, ClusterCenter center) {
		super();
		this.canopyId = new IntWritable(canopyId);
		this.center = new Vector(center.center, center.center.getName());
		
	}

	public ClusterCenter(int canopyId, Vector center) {
		super();
		this.canopyId = new IntWritable(canopyId);
		this.center = center;
	}

	public boolean converged(ClusterCenter c) {
		return compareTo(c) == 0 ? false : true;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		canopyId.write(out);
		center.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.canopyId = new IntWritable();
		canopyId.readFields(in);
		this.center = new Vector();
		center.readFields(in);
	}

	@Override
	public int compareTo(ClusterCenter o) {
		return center.compareTo(o.getCenter());
	}
	
	@Override
	public boolean equals(Object obj) {
		if (obj instanceof ClusterCenter) {
			ClusterCenter o = (ClusterCenter)obj;
			
			return center.equals(o.center);
		}
		return false;
	}
	
	@Override
	public int hashCode() {
		return center.hashCode();
	}


	/**
	 * @return the center
	 */
	public Vector getCenter() {
		return center;
	}
	
	public int getCanopyId() {
		return canopyId.get();
	}
	
	public void setCanopyId(int id) {
		 this.canopyId.set(id);
	}

	@Override
	public String toString() {
		//return "ClusterCenter [canopyId=" + canopyId + " center=" + center + "]";
		return center.toString();
	}

}
