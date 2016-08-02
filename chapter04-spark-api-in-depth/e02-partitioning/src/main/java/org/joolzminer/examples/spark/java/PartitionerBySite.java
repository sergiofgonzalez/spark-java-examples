package org.joolzminer.examples.spark.java;

import java.util.List;

import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaPairRDD;

@SuppressWarnings("serial")
public class PartitionerBySite<K> extends Partitioner {

	private int numPartitions;
	private List<K> keys;
	
	private PartitionerBySite() {		
	}
	
	public static <K> PartitionerBySite<K> create(Class<K> clazz) {
		return new PartitionerBySite<K>();
	}
	
	public <V> PartitionerBySite<K> from(JavaPairRDD<K,V> jPairRDD) {
		this.numPartitions = (int) jPairRDD.keys().distinct().count();
		this.keys = jPairRDD.keys().collect();
		return this;
	}	
	
	public Partitioner build() {
		return (Partitioner) this;
	}
	
	@Override
	public int getPartition(Object key) {	
		return keys.indexOf((String)key);
	}

	@Override
	public int numPartitions() {
		return numPartitions;
	}
}
