package models;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class Vector implements WritableComparable<Vector> {

	private Text name;
	private double[] vector;

	public Vector() {
		super();
		vector = new double[0];
		this.name = new Text();
	}

	public Vector(String name) {
		super();
		this.name = new Text(name);
	}
	
	public Vector(Vector v) {
		super();
		this.name = new Text();
		int l = v.vector.length;
		this.vector = new double[l];
		System.arraycopy(v.vector, 0, this.vector, 0, l);
	}
	
	public Vector(Vector v, String name) {
		super();
		this.name = new Text(name);
		int l = v.vector.length;
		this.vector = new double[l];
		System.arraycopy(v.vector, 0, this.vector, 0, l);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		name.write(out);
		out.writeInt(vector.length);
		for (int i = 0; i < vector.length; i++)
			out.writeDouble(vector[i]);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		name.readFields(in);
		int size = in.readInt();
		vector = new double[size];
		for (int i = 0; i < size; i++)
			vector[i] = in.readDouble();
	}

	@Override
	public int compareTo(Vector o) {

		boolean equals = true;
		for (int i = 0; i < vector.length; i++) {
			double c = vector[i] - o.vector[i];
			if (c!= 0.0d)
			{
				return (int)c;
			}		
		}
		return 0;
	}

	public double[] getVector() {
		return vector;
	}

	public void setVector(double[] vector) {
		this.vector = vector;
	}
	
	public String getName() {
		return this.name.toString();
	}
	
	@Override
	public boolean equals(Object obj) {
		if (obj instanceof Vector) {
			Vector o = (Vector)obj;
			
			for (int i = 0; i < vector.length; i++) {
				if (vector[i] != o.vector[i])
				{
					return false;
				}		
			}
			return true;
		}
		return false;
	}
	
	@Override
	public int hashCode() {
		return name.hashCode();//vector.hashCode();
	}


	@Override
	public String toString() {
		return name + " " + Arrays.toString(vector);
	}

}
