package models;

public class DistanceMeasurer {

	/**
	 * Manhattan stuffz.
	 * 
	 * @param center
	 * @param v
	 * @return
	 */
	public static final double measureDistance(ClusterCenter center, Vector v) {
		double sum = 0;
		int length = v.getVector().length;
		for (int i = 0; i < length; i++) {
			sum += Math.abs(center.getCenter().getVector()[i]
					- v.getVector()[i]);
		}

		return sum;
	}
	
	public static final double measureDistance(ClusterCenter c1, ClusterCenter c2) {
		double sum = 0;
		int length = c2.getCenter().getVector().length;
		for (int i = 0; i < length; i++) {
			sum += Math.abs(c1.getCenter().getVector()[i]
					- c2.getCenter().getVector()[i]);
		}

		return sum;
	}
}
